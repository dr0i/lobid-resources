/* Copyright 2015  hbz, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.resources;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.culturegraph.mf.stream.converter.RecordReader;
import org.culturegraph.mf.stream.source.DirReader;
import org.culturegraph.mf.stream.source.FileOpener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHits;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.hp.hpl.jena.rdf.model.Model;

/**
 * Read a directory with records got from lobid.org in ntriple serialization.
 * The records are indexed as JSON-LD in an in-memory elasticsearch instance,
 * then queried and concatenated into two files:
 * <ul>
 * <li>one json file, reflecting the source field of elasticsearch
 * <li>one ntriple file, which is great to make diffs upon
 * </ul>
 * For testing, diffs are done against these files.
 * 
 * @author Pascal Christoph (dr0i)
 * 
 */
@SuppressWarnings("javadoc")
public final class LobidResourcesAsNtriples2ElasticsearchLobidTest {
	private static Node node;
	protected static Client client;

	private static final String INDEX_NAME_LOBID_RESOURCES = "lobid-resources-"
			+ (new SimpleDateFormat("yyyyMMdd-hhmmss").format(new Date()));
	private static final String N_TRIPLE = "N-TRIPLE";
	private static final String TEST_FILENAME_NTRIPLES = "hbz01.es.nt";
	private static final String TEST_FILENAME_JSON =
			"src/test/resources/hbz01.es.json";

	@BeforeClass
	public static void setup() {
		node = org.elasticsearch.node.NodeBuilder.nodeBuilder().local(true)
				.settings(ImmutableSettings.settingsBuilder()
						.put("index.number_of_replicas", "0")
						.put("index.number_of_shards", "1").build())
				.node();
		client = node.client();
		client.admin().indices().prepareDelete("_all").execute().actionGet();
		client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute()
				.actionGet();
	}

	@SuppressWarnings("static-method")
	@Test
	public void testFlow() throws JsonLdError {
		buildAndExecuteFlow();
		SearchHits sh =
				ElasticsearchDocuments.getElasticsearchDocuments().getHits();
		System.out.println("Total hits: " + sh.getTotalHits());
		try {
			writeFileAndTestJson(TEST_FILENAME_JSON, JsonUtils.toPrettyString(
					JsonUtils.fromString(ElasticsearchDocuments.getAsJsonArray()[0])));
		} catch (IOException e) {
			e.printStackTrace();
		}
		// writeFileAndTest(TEST_FILENAME_NTRIPLES,
		// ElasticsearchDocuments.getAsNtriples());
	}

	private static void writeFileAndTestJson(String testFilenameJson,
			String prettyString) throws JsonLdError {
		// Object frame = null;
		// try {
		// frame = JsonUtils.fromInputStream(
		// FileUtils.openInputStream(new File("src/main/resources/frame.json")));
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		try {
			FileUtils.write(new File("hbz01.es.json"), prettyString);
			ObjectMapper mapper = new ObjectMapper();

			System.out.println(prettyString);
			// generated data
			JsonNode jp =
					mapper.getFactory().createParser(prettyString).readValueAsTree();
			HashMap<String, String> generatedMap = new HashMap<>();
			traverse(jp, generatedMap);
			generatedMap.forEach((key, val) -> System.out
					.println("GeneratedKEY=" + key + " VALUE=" + val));

			// test data
			jp = mapper.getFactory()
					.createParser(
							FileUtils.readFileToString(new File(TEST_FILENAME_JSON)))
					.readValueAsTree();
			HashMap<String, String> testMap = new HashMap<>();
			traverse(jp, testMap);
			testMap.forEach(
					(key, val) -> System.out.println("TestKEY=" + key + " VALUE=" + val));

			// frame
			jp = mapper.getFactory()
					.createParser(FileUtils
							.readFileToString(new File("src/main/resources/frame.json")))
					.readValueAsTree();
			HashMap<String, String> frameMap = new HashMap<>();
			traverse(jp, frameMap);
			// list of properties which are of type '@list' (and thus ordered lists)
			List<String> orderListKeys = frameMap.entrySet().parallelStream()
					.filter(e -> e.getValue().equals("\"@list\"")).flatMap(e -> Stream
							.of(e.getKey().split(", ")).filter(s -> !s.matches("\\[?@.*")))
					.collect(Collectors.toList());

			orderListKeys.forEach(System.out::println);
			System.out.println("\n##### removed good entries ###");
			for (Entry<String, String> e : testMap.entrySet()) {
				System.out.println("compare! " + e.getKey());
				if (orderListKeys.contains(e.getKey())) {
					System.out.println("  contained! " + e.getKey());
				} else {
					if (compareValue(e.getValue(), generatedMap.get(e.getKey()))) {
						generatedMap.remove(e.getKey());
						System.out.println("   removed " + e.getKey());
					} else
						System.out.println("  else! " + e.getKey());
				}
			}
			generatedMap.forEach(
					(key, val) -> System.out.println("KEY=" + key + " VALUE=" + val));

			System.out.println(testMap.equals(generatedMap));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static boolean compareValue(String a, String b) {
		boolean ret = false;
		// get rid of enclosing quote sign
		final String aa = a.substring(1, a.length() - 1);
		final String bb = b.substring(1, b.length() - 1);
		System.out.println("a= " + a + "\nb=" + b);

		if (Arrays.stream(aa.split("\","))
				.ranyMatch(e -> Arrays.stream(bb.split("\",")).peek(System.out::println)
						.collect(Collectors.toSet()).contains(e)))
			ret = true;
		System.out.println("ret= " + ret);
		return ret;
	}

	static Stack<String> stack = new Stack<>();

	public static void traverse(JsonNode jnode, HashMap<String, String> map) {
		if (jnode.getNodeType().equals(JsonNodeType.OBJECT)) {
			Iterator<Map.Entry<String, JsonNode>> it = jnode.fields();
			while (it.hasNext()) {
				Map.Entry<String, JsonNode> entry = it.next();
				stack.push(entry.getKey());
				traverse(entry.getValue(), map);
				stack.pop();
			}
		} else if (jnode.isArray()) {
			Iterator<JsonNode> it = jnode.iterator();
			while (it.hasNext())
				traverse(it.next(), map);
		} else if (jnode.isValueNode()) {
			if (map.containsKey(stack.toString())) {
				String a = map.get(stack.toString()).concat("," + jnode.toString());
				map.put(stack.toString(), a);
			} else
				map.put(stack.toString(), jnode.toString());
		}
	}

	private static void writeFileAndTest(final String TEST_FILENAME,
			final String DOCUMENTS) {
		File testFile = new File(TEST_FILENAME);
		try {
			FileUtils.writeStringToFile(testFile, DOCUMENTS, false);
			AbstractIngestTests.compareFilesDefaultingBNodes(testFile,
					new File(Thread.currentThread().getContextClassLoader()
							.getResource(TEST_FILENAME).toURI()));

		} catch (IOException | URISyntaxException e) {
			e.printStackTrace();
		}
		// testFile.deleteOnExit();
	}

	private static void buildAndExecuteFlow() {
		final DirReader dirReader = new DirReader();
		final FileOpener opener = new FileOpener();
		final Triples2RdfModel triple2model = new Triples2RdfModel();
		triple2model.setInput(N_TRIPLE);
		RecordReader lr = new RecordReader();
		dirReader.setReceiver(opener).setReceiver(lr).setReceiver(triple2model)
				.setReceiver(new RdfModel2ElasticsearchJsonLd())
				.setReceiver(getElasticsearchIndexer());
		dirReader.process(
				new File("src/test/resources/hbz01RecordsInput/").getAbsolutePath());
		opener.closeStream();
		dirReader.closeStream();
	}

	private static ElasticsearchIndexer getElasticsearchIndexer() {
		ElasticsearchIndexer esIndexer = new ElasticsearchIndexer();
		esIndexer.setElasticsearchClient(client);
		esIndexer.setIndexAliasSuffix("");
		// esIndexer.setClustername("lobid-hbz");
		// esIndexer.setHostname("quaoar1.hbz-nrw.de");
		esIndexer.setIndexName(INDEX_NAME_LOBID_RESOURCES);
		esIndexer.setUpdateNewestIndex(false);
		esIndexer.onSetReceiver();
		client = esIndexer.client;
		return esIndexer;
	}

	@AfterClass
	public static void down() {
		// client.admin().indices().prepareDelete(INDEX_NAME_LOBID_RESOURCES).execute()
		// .actionGet();
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		client.close();
	}

	static class ElasticsearchDocuments {
		private static final JsonLdOptions JsonLdTripleCallback = null;

		static private SearchResponse getElasticsearchDocuments() {
			return client.prepareSearch(INDEX_NAME_LOBID_RESOURCES)
					.setQuery(new MatchAllQueryBuilder()).setFrom(0).setSize(10000)
					.execute().actionGet();
		}

		static String getAsNtriples() {
			return Arrays.asList(getElasticsearchDocuments().getHits().getHits())
					.parallelStream()
					.flatMap(hit -> Stream.of(toRdf(hit.getSourceAsString())))
					.collect(Collectors.joining());
		}

		static String[] getAsJsonArray() {
			return Arrays.stream(getElasticsearchDocuments().getHits().getHits())
					.flatMap(hit -> Stream.of(hit.getSourceAsString()))
					.toArray(String[]::new);
		}

		private static String toRdf(final String jsonLd) {
			try {
				final Object jsonObject = JsonUtils.fromString(jsonLd);
				final Model model =
						(Model) JsonLdProcessor.toRDF(jsonObject, JsonLdTripleCallback);
				final StringWriter writer = new StringWriter();
				model.write(writer, N_TRIPLE);
				return writer.toString();
			} catch (IOException | JsonLdError e) {
				e.printStackTrace();
			}
			return null;
		}
	}
}
