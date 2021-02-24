package edu.stanford.protege.obo;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.google.common.io.CountingInputStream;
import org.obolibrary.obo2owl.OWLAPIObo2Owl;
import org.obolibrary.oboformat.model.Frame;
import org.obolibrary.oboformat.model.FrameMergeException;
import org.obolibrary.oboformat.model.OBODoc;
import org.obolibrary.oboformat.parser.OBOFormatParser;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLDeclarationAxiom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2020-11-03
 * @noinspection UnstableApiUsage
 */
public class MinimalOboParser {

    private final static Logger logger = LoggerFactory.getLogger(MinimalOboParser.class);

    private final Consumer<OWLAxiom> axiomConsumer;


    public MinimalOboParser(Consumer<OWLAxiom> axiomConsumer) {
        this.axiomConsumer = checkNotNull(axiomConsumer);
    }

    public void parse(@Nonnull InputStream inputStream) throws IOException {
        parse(inputStream, -1);
    }

    public void parse(@Nonnull InputStream inputStream,
                      long streamLength) throws IOException {
        var in = inputStream instanceof CountingInputStream ? (CountingInputStream) inputStream : new CountingInputStream(inputStream);

        var csvTranslator = new MinimalObo2Owl(in, axiomConsumer, streamLength, false);

        var sw = Stopwatch.createStarted();

        var oboParser = new OBOFormatParser();
        replaceStringCacheWithNoOpCache(oboParser);
        var bufferedReader = new BufferedReader(new InputStreamReader(in));
        oboParser.setReader(bufferedReader);

        var obodoc = new MinimalOboDoc();
        obodoc.setTranslator(csvTranslator);
        csvTranslator.setObodoc(obodoc);
        oboParser.parseOBODoc(obodoc);

        logger.info("Time: %,dms\n", sw.elapsed(TimeUnit.MILLISECONDS));
        logger.info("Axioms: %,d\n", +csvTranslator.getAxiomsCount());

        bufferedReader.close();
    }

    private void replaceStringCacheWithNoOpCache(OBOFormatParser parser) {
        try {
            var parserClass = parser.getClass();
            var stringCache = Stream.of(parserClass.getDeclaredFields())
                                    .filter(field -> field.getName().equals("stringCache"))
                                    .findFirst()
                                    .orElseThrow();
            stringCache.setAccessible(true);
            stringCache.set(parser, new NoOpLoadingCache());
        } catch (IllegalAccessException e) {
            logger.warn(
                    "WARN: Unable to replace LoadingCache with No-op Cache. This will cause the loading to be slower.");
        }
    }

    private static class MinimalObo2Owl extends OWLAPIObo2Owl {

        private final AtomicInteger counter = new AtomicInteger();

        private final CountingInputStream countingInputStream;

        private final long fileSize;

        private final Consumer<OWLAxiom> csvExporter;

        private final boolean isTrackingDeclaration;

        private final Set<OWLDeclarationAxiom> declarationCache = Sets.newHashSet();

        private long ts = ManagementFactory.getThreadMXBean().getCurrentThreadUserTime();

        public MinimalObo2Owl(@Nonnull CountingInputStream countingInputStream,
                              @Nonnull Consumer<OWLAxiom> csvExporter,
                              long fileSize,
                              boolean isTrackingDeclaration) {
            super(OWLManager.createOWLOntologyManager());
            this.countingInputStream = countingInputStream;
            this.csvExporter = csvExporter;
            this.fileSize = fileSize;
            this.isTrackingDeclaration = isTrackingDeclaration;
        }

        @Override
        protected void add(Set<OWLAxiom> axioms) {
            if (axioms != null) {
                axioms.forEach(this::add);
            }
        }

        @Override
        protected void add(OWLAxiom axiom) {
            if (axiom instanceof OWLDeclarationAxiom) {
                if (isTrackingDeclaration) {
                    if (declarationCache.add((OWLDeclarationAxiom) axiom)) {
                        addAxiom(axiom);
                    }
                }
                else {
                    addAxiom(axiom);
                }
            }
            else {
                addAxiom(axiom);
            }
        }

        private void addAxiom(OWLAxiom axiom) {
            counter.incrementAndGet();
            printLog();
            csvExporter.accept(axiom);
        }

        private void printLog() {
            var c = counter.get();
            if (c % 1_000_000 == 0) {
                //                    var csvWriter = csvExporter.getCsvWriter();
                long read = countingInputStream.getCount() / (1024 * 1024);
                long ts1 = ManagementFactory.getThreadMXBean().getCurrentThreadUserTime();
                long delta = (ts1 - ts) / 1000_000;
                ts = ts1;
                double percentage = (countingInputStream.getCount() * 100.0) / fileSize;
                var runtime = Runtime.getRuntime();
                var totalMemory = runtime.totalMemory();
                var freeMemory = runtime.freeMemory();
                var consumedMemory = (totalMemory - freeMemory) / (1024 * 1024);
                var percentageRead = fileSize == -1 ? 0 : percentage;
                System.out.printf("%,9d axioms (Read %,4d Mb [%3d%%]  Delta: %,5d ms) (Used memory: %,8d MB)\n",
                                  c,
                                  read,
                                  (int) percentageRead,
                                  delta,
                                  consumedMemory);
            }
        }

        public int getAxiomsCount() {
            return counter.get();
        }

        @Nonnull
        @Override
        public IRI oboIdToIRI(@Nonnull String id) {
            return oboIdToIRI_load(id);
        }
    }

    private static class MinimalOboDoc extends OBODoc {

        private OWLAPIObo2Owl translator;

        public void setTranslator(OWLAPIObo2Owl translator) {
            this.translator = translator;
        }

        @Override
        public void addFrame(@Nonnull Frame f) throws FrameMergeException {
            if (f.getType().equals(Frame.FrameType.TYPEDEF)) {
                super.addFrame(f);
            }
            if (f.getType().equals(Frame.FrameType.TERM)) {
                translator.trTermFrame(f);
            }
        }
    }

    private static class NoOpLoadingCache implements LoadingCache<String, String> {

        @Override
        public String get(@Nonnull String key) {
            return key;
        }

        @Nullable
        @Override
        public Map<String, String> getAll(@Nonnull Iterable<? extends String> keys) {
            return null;
        }

        @Override
        public void refresh(@Nonnull String key) {
            // NO-OP
        }

        @Nullable
        @Override
        public String getIfPresent(@Nonnull Object key) {
            return null;
        }

        @Nullable
        @Override
        public String get(@Nonnull String key, @Nonnull Function<? super String, ? extends String> mappingFunction) {
            return null;
        }

        @Nullable
        @Override
        public Map<String, String> getAllPresent(@Nonnull Iterable<?> keys) {
            return null;
        }

        @Override
        public void put(@Nonnull String key, @Nonnull String value) {
            // NO-OP
        }

        @Override
        public void putAll(@Nonnull Map<? extends String, ? extends String> map) {
            // NO-OP
        }

        @Override
        public void invalidate(@Nonnull Object key) {
            // NO-OP
        }

        @Override
        public void invalidateAll(@Nonnull Iterable<?> keys) {
            // NO-OP
        }

        @Override
        public void invalidateAll() {
            // NO-OP
        }

        @Override
        public long estimatedSize() {
            return 0;
        }

        @Nullable
        @Override
        public CacheStats stats() {
            return null;
        }

        @Nullable
        @Override
        public ConcurrentMap<String, String> asMap() {
            return null;
        }

        @Override
        public void cleanUp() {
            // NO-OP
        }

        @Nullable
        @Override
        public Policy<String, String> policy() {
            return null;
        }
    }

}
