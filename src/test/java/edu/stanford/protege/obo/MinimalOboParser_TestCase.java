package edu.stanford.protege.obo;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.semanticweb.owlapi.apibinding.OWLFunctionalSyntaxFactory;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.function.Consumer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.semanticweb.owlapi.apibinding.OWLFunctionalSyntaxFactory.*;

@RunWith(MockitoJUnitRunner.class)
public class MinimalOboParser_TestCase {

    private MinimalOboParser parser;

    @Mock
    private Consumer<OWLAxiom> axiomConsumer;

    private String input = "[Term]\n"
            + "id: GO:0000001\n"
            + "is_a: GO:0048311 ! mitochondrion distribution";

    private OWLSubClassOfAxiom expectedAxiom = SubClassOf(Class(IRI.create("http://purl.obolibrary.org/obo/GO_0000001")),
                                                      Class(IRI.create("http://purl.obolibrary.org/obo/GO_0048311")));

    @Before
    public void setUp() throws Exception {
        parser = new MinimalOboParser(axiomConsumer);
    }

    @Test
    public void shouldParseWithoutFileLength() throws IOException {
        parser.parse(new ByteArrayInputStream(input.getBytes(Charset.forName("utf-8"))));
        verify(axiomConsumer, times(1)).accept(expectedAxiom);
    }

    @Test
    public void shouldParseWithFileLength() throws IOException {
        parser.parse(new ByteArrayInputStream(input.getBytes(Charset.forName("utf-8"))), input.length());
        verify(axiomConsumer, times(1)).accept(expectedAxiom);
    }
}