package net.griddynamics.solrtexttagger.impl;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by fsoloviev on 11/11/15.
 */
public class TagFileLineCallable implements Callable<String> {

    String fields[];
    String phrase;
    private static SolrClient solr;

    public TagFileLineCallable(String[] fields, String phrase, SolrClient solr) {
        this.fields = fields;
        this.phrase = phrase;
        this.solr = solr;
    }

    @Override
    public String call() throws Exception {
        return getTaggedFileString(fields, phrase);
    }

    private static String getTaggedFileString(String[] fields, String line) throws InterruptedException {

        List<NamedList> fieldSolrTags = new ArrayList<>();
        for (String field : fields) {
            fieldSolrTags.addAll(getSolrTags(field, line));
        }

        TagUtils.sortSolrTags(fieldSolrTags);

        List<NamedList> unknownTags = TagUtils.getUnknownTags(fieldSolrTags, line);

        fieldSolrTags.addAll(unknownTags);

        TagUtils.sortSolrTags(fieldSolrTags);

        String commaSeparatedTags = TagUtils.tagsToString(fieldSolrTags);

        return commaSeparatedTags;
    }

    private static List<NamedList> getSolrTags(String field, String str) {
        SolrParams q = new SolrQuery()
                .setRequestHandler("/tag")
                .set("overlaps", "NO_SUB")
                .set("tagsLimit", "5000")
                .set("fl", "*")
                .set("matchText", "true")
                .set("stream.body", str)
                .set("field", field)
                .set("rows", "0");

        QueryResponse queryResponse = null;

        try {
            queryResponse = solr.query(q);
        } catch (SolrServerException e) {
            throw new RuntimeException(e);
        }

        List<NamedList> markedSolrTags = TagUtils.markTagsByField((List<NamedList>) queryResponse.getResponse().get("tags"),
                field.replace("_concept", ""));

        return markedSolrTags;
    }

}
