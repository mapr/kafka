public class PomUtil {
    public static final List<String> forbiddenElements = [ "project",
        "modelVersion",
        "artifactId",
        "groupId",
        "version"
    ]

    public static boolean listContains(String checkedString) {
        for (String masterString : forbiddenElements) {
            if (checkedString.contains(masterString)) {
                return true
            }
        }
        return false
    }

    public static List<Node> parsePom(File originalPom) {
        List<Node> cleanedNodes = new Vector<String>()
        Node originalPomNode = new XmlParser().parseText(originalPom.text)
        originalPomNode.children().each {
            Node originalElement ->
            String elementName = originalElement.name().toString()
            if (!(listContains(elementName))) {
                cleanedNodes.add(originalElement)
            }
        }
        return cleanedNodes
    }

    public static List<Node> parsePom(File originalPom, String maprVersion) {
        List<Node> cleanedNodes = parsePom(originalPom)
        for (Node node : cleanedNodes) {
            if (node.name().toString().contains("dependencies")) {
                if ((maprVersion == null) ||
                    (maprVersion.equalsIgnoreCase("null")) ||
                    (maprVersion.contains("null")) ||
                    (maprVersion.contains("NULL")) ||
                    (maprVersion.equalsIgnoreCase("0")) ||
                    (maprVersion.equalsIgnorecase(""))
                ) {
                    println "not adding MapR Streams dependency"
                } else {
                    node.append(maprStreams(maprVersion))
                }
            }
        }
        return cleanedNodes
    }

    public static List<Node> parsePom(String originalPom) {
        File originalPomFile = new File(originalPom)
        return parsePom(originalPomFile)
    }
    
    public static Node maprStreams(String maprVersion) {
        Node mrsNode = new NodeBuilder().dependency {
            groupId "com.mapr.streams"
            artifactId "mapr-streams"
            version maprVersion
        }
        return mrsNode
    }
}
