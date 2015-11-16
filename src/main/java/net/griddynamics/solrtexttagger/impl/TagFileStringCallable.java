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
public class TagFileStringCallable implements Callable<String> {

    String fields[];
    String phrase;
    private static SolrClient solr;

    public TagFileStringCallable(String[] fields, String phrase, SolrClient solr) {
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

        sortSolrTags(fieldSolrTags);

        List<NamedList> unknownTags = getUnknownTags(fieldSolrTags, line);

        fieldSolrTags.addAll(unknownTags);

        sortSolrTags(fieldSolrTags);

        String commaSeparatedTags = toString(fieldSolrTags);

        return commaSeparatedTags;
    }

    private static String toString(List<NamedList> fields) {
        StringBuilder sb = new StringBuilder();
        fields.forEach(field -> {
            sb.append(field.get("fieldName"));
            sb.append(",");
            sb.append(field.get("matchText"));
            sb.append(",");
        });
        if(sb.length() > 0)
            sb.deleteCharAt(sb.length()-1);
        return sb.toString();
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

        List<NamedList> markedSolrTags = markTagsByField((List<NamedList>)queryResponse.getResponse().get("tags"),
                field.replace("_concept",""));

        return markedSolrTags;
    }

    private static List<NamedList> markTagsByField(List<NamedList> solrTags, String fieldName) {
        solrTags.forEach(solrTag -> {
            solrTag.add("fieldName", fieldName);
        });
        return solrTags;
    }

    private static List<NamedList> sortSolrTags(List<NamedList> solrTags){
        solrTags.sort((o1, o2) -> {
                    Integer offsetCompare = ((Integer) o1.get("startOffset")).compareTo((Integer) o2.get("startOffset"));
                    if (offsetCompare.equals(0)) {
                        return ((Integer) ((String) o1.get("matchText")).length()).compareTo(((String) o2.get("matchText")).length());
                    } else
                        return offsetCompare;
                }
        );
        return solrTags;
    }

    private static List<NamedList> getUnknownTags(List<NamedList> solrTags, String str) {
        List<NamedList> unknownTags = new ArrayList<>();
        if (solrTags.isEmpty()) {
            addTagToList(unknownTags,0,str.length(),str);
        }
        if (!solrTags.isEmpty()) {
            Integer firstTagStartOffset = (Integer)solrTags.get(0).get("startOffset");
            if (firstTagStartOffset > 0)
                addTagToList(unknownTags,0,firstTagStartOffset, str);
        }
        for (int i = 1; i < solrTags.size(); i++) {
            Integer currentTagStartOffset = (Integer)solrTags.get(i).get("startOffset");
            Integer previousTagEndOffset = (Integer)solrTags.get(i - 1).get("endOffset");
            if (currentTagStartOffset - previousTagEndOffset > 1) {
                addTagToList(unknownTags,previousTagEndOffset,currentTagStartOffset,str);
            }
        }
        if (!solrTags.isEmpty()) {
            NamedList lastTag = solrTags.get(solrTags.size() - 1);
            Integer lastTagEndOffset = (Integer)lastTag.get("endOffset");
            if (lastTagEndOffset < str.length()) {
                addTagToList(unknownTags,lastTagEndOffset,str.length(),str);
            }
        }
        markTagsByField(unknownTags, "UNKNOWN");
        return unknownTags;
    }

    private static void addTagToList(List<NamedList> tagList, Integer startOffset, Integer endOffset, String str) {
        String matchText = str.substring(startOffset, endOffset).trim();
        if (!matchText.isEmpty()) {
            NamedList newTag = createTag(startOffset, endOffset, matchText);
            tagList.add(newTag);
        }
    }

    private static NamedList createTag(Integer startOffset, Integer endOffset, String matchText) {
        NamedList newTag = new NamedList();
        newTag.add("startOffset", startOffset);
        newTag.add("endOffset", endOffset);
        newTag.add("matchText", matchText);
        return newTag;
    }
}
