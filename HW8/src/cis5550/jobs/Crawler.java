package cis5550.jobs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import java.util.ArrayList;
import java.util.List;
import cis5550.flame.*;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

public class Crawler {
    // public static void run(FlameContext context, String[] args) {
    // if (args.length != 1) {
    // context.output("Error: Provide a single seed URL as an argument.");
    // } else {
    // context.output("OK");
    // String url = args[0];
    // List<String> urlList = new ArrayList<>();
    // urlList.add(url);
    // FlameRDDImpl rdd = (FlameRDDImpl) context.parallelize(urlList);
    // while (rdd.count() != 0) {
    // rdd = (FlameRDDImpl) rdd.flatMap(urls -> {
    // List<String> results = new ArrayList<>();
    // try {
    // URL urlObj = new URL(url);
    // HttpURLConnection connection = (HttpURLConnection) urlObj.openConnection();
    // connection.setRequestMethod("GET");
    // connection.connect();
    // int responseCode = connection.getResponseCode();
    //
    // if (responseCode == 200) {
    // BufferedReader reader = new BufferedReader(
    // new InputStreamReader(connection.getInputStream()));
    // String line;
    // StringBuilder content = new StringBuilder();
    // while ((line = reader.readLine()) != null) {
    // content.append(line);
    // }
    // reader.close();
    // Row r = new Row(Hasher.hash(url));
    // r.put("url",url);
    // r.put("page", content.toString());
    // // You should adapt this part to upload content to KVS
    // context.getKVS().putRow("pt-crawl",r);
    // results = extracturl(content.toString());
    // // For demonstration, we add a sample new URL
    //// results.add("http://example.com/new-page");
    // String[] seedurl = URLParser.parseURL(url);
    // int lastIndex = seedurl[3].lastIndexOf("/");
    // String path = "/";
    // if (lastIndex != -1) {
    // path = seedurl[3].substring(0, lastIndex + 1);
    // }
    // String beforepath="";
    // if (seedurl[2]!=null) {
    // beforepath=seedurl[0]+"//"+seedurl[1]+":"+seedurl[2];
    // }else {
    // if(seedurl[0].equals("http")) {
    // beforepath=seedurl[0]+"//"+seedurl[0]+":"+"80";
    // }else {
    // beforepath=seedurl[0]+"//"+seedurl[1]+":"+"8000";
    // }
    // }
    //
    // for(String rawurl:results) {
    // int fragmentIndex = rawurl.indexOf("#");
    // if (fragmentIndex != -1) {
    // rawurl = rawurl.substring(0, fragmentIndex);
    // }else {
    // rawurl = url;
    // continue;
    // }
    // if (rawurl.startsWith("/")) {
    // rawurl = beforepath+rawurl;
    // continue;
    // }
    // else {
    // rawurl = beforepath+path+rawurl;
    // }
    // while (rawurl.contains("..")) {
    // int index = rawurl.indexOf("..");
    // int slashIndex = rawurl.lastIndexOf('/', index - 2);
    // if (slashIndex != -1) {
    // rawurl = rawurl.substring(0, slashIndex) + rawurl.substring(index + 2);
    // } else {
    // break;
    // }
    // }
    //
    // }
    //
    //
    //
    // }
    // } catch (Exception e) {
    // e.printStackTrace();
    // }
    // return results;
    // });
    //
    // }
    // }
    // }

    public static void run(FlameContext context, String[] args) {
        if (args.length != 1) {
            context.output("Error: Provide a single seed URL as an argument.");
        } else {

            context.output("OK");
            String url = args[0];
            context.output(url);
            List<String> urlList = new ArrayList<>();
            urlList.add(url);
            context.output(url);
            try {
                FlameRDDImpl rdd = (FlameRDDImpl) context.parallelize(urlList);
                String[] seedurl1 = URLParser.parseURL(url);
                addhostrecord(context, seedurl1);
                context.output(rdd.count() + "");
                context.getKVS().delete("visit-history");
                context.getKVS().delete("pt-crawl");
                while (rdd.count() != 0) {

                    System.out.println("thecount:" + rdd.count() + "");

                    rdd = (FlameRDDImpl) rdd.flatMap(urls -> {
                        List<String> results = new ArrayList<>();
                        String[] seedurl = URLParser.parseURL(urls);
                        Row visitrecord = context.getKVS().getRow("pt-crawl", Hasher.hash(urls));
                        Row hostrecord = context.getKVS().getRow("visit-history", Hasher.hash(seedurl[1] + seedurl[2]));
                        if (visitrecord != null) {
                            if (visitrecord.get("responseCode").equals("200")) {
                                return results;
                            }
                        }
                        if (hostrecord == null) {
                            addhostrecord(context, seedurl);
                        }
                        hostrecord = context.getKVS().getRow("visit-history", Hasher.hash(seedurl[1] + seedurl[2]));
                        String allowed = hostrecord.get("allow");
                        String disallowed = hostrecord.get("disallow");
                        int isallowed = 1;
                        for (String i : disallowed.split(",")) {
                            if (seedurl[3].startsWith(i)) {
                                isallowed = 0;
                                break;
                            }
                        }
                        if (isallowed == 0) {
                            for (String i : allowed.split(",")) {
                                if (seedurl[3].startsWith(i)) {
                                    isallowed = 1;
                                    break;
                                }
                            }
                        }
                        if (isallowed == 0) {
                            return results;
                        }

                        try {
                            URL urlObj = new URL(urls);
                            // String[] seedurl = URLParser.parseURL(urls);
                            Row curtimerow = context.getKVS().getRow("visit-history",
                                    Hasher.hash(seedurl[1] + seedurl[2]));
                            long currentTime = System.currentTimeMillis();
                            long timeValue = Long.parseLong(curtimerow.get("time"));
                            double delayValue = Double.parseDouble(curtimerow.get("delay"));

                            if ((currentTime - timeValue) <= (delayValue * 1000)) {
                                results.add(urls);
                                return results;
                            } else {
                                curtimerow.put("time", System.currentTimeMillis() + "");
                            }

                            HttpURLConnection headconnection = (HttpURLConnection) urlObj.openConnection();
                            headconnection.setRequestMethod("HEAD");
                            // headconnection.setRequestHEAD("cis5550-crawler");
                            // headconnection.
                            headconnection.connect();
                            int headResponseCode = headconnection.getResponseCode();

                            // Check if the HEAD response code is 200 and content type is acceptable
                            if (headResponseCode == 200) {
                                URL urlOb = new URL(urls);
                                HttpURLConnection connection = (HttpURLConnection) urlOb.openConnection();
                                connection.setRequestMethod("GET");
                                connection.connect();
                                int responseCode = connection.getResponseCode();
                                System.out.print("code" + urls + ":" + responseCode);
                                if (responseCode == 200) {
                                    seedurl = URLParser.parseURL(urls);
                                    if (seedurl[0] == null || (!seedurl[0].toLowerCase().equals("http")
                                            && !seedurl[0].toLowerCase().equals("https"))) {
                                        return results;
                                    }
                                    int lastIndex = seedurl[3].lastIndexOf("/");
                                    String path = "/";
                                    if (lastIndex != -1) {
                                        path = seedurl[3].substring(0, lastIndex + 1);
                                    }
                                    if (seedurl[3] != null && (seedurl[3].endsWith(".jpg")
                                            || seedurl[3].endsWith(".jpeg") || seedurl[3].endsWith(".gif")
                                            || seedurl[3].endsWith(".png") || seedurl[3].endsWith(".txt"))) {
                                        return results;
                                    }
                                    String beforepath = "";

                                    if (seedurl[2] != null) {
                                        beforepath = seedurl[0] + "://" + seedurl[1] + ":" + seedurl[2];
                                    } else {
                                        if (seedurl[0].toLowerCase().equals("http")) {
                                            beforepath = seedurl[0] + "://" + seedurl[1] + ":" + "80";
                                        } else if (seedurl[0].toLowerCase().equals("https")) {
                                            beforepath = seedurl[0] + "://" + seedurl[1] + ":" + "8000";
                                        } else {
                                            return results;
                                        }
                                    }

                                    Row r = new Row(Hasher.hash(urls));

                                    BufferedReader reader = new BufferedReader(
                                            new InputStreamReader(connection.getInputStream()));
                                    String line;
                                    StringBuilder content = new StringBuilder();
                                    while ((line = reader.readLine()) != null) {
                                        content.append(line);
                                    }
                                    reader.close();

                                    if (connection.getContentType() != null) {
                                        r.put("contentType", connection.getContentType());
                                        if (connection.getContentType().startsWith("text/html")) {
                                            r.put("page", content.toString());
                                        }
                                    }
                                    if (connection.getContentLength() != -1) {
                                        r.put("Length", "" + connection.getContentLength());
                                    }

                                    r.put("url", urls);
                                    r.put("responseCode", "" + responseCode);

                                    context.getKVS().putRow("pt-crawl", r);
                                    System.out.print("add" + urls + "to pt");
                                    results = extracturl(content.toString());
                                    System.out.println(urls + "has" + results);

                                    List<String> updatedResults = new ArrayList<>();
                                    for (String rawurl : results) {
                                        // System.out.println(rawurl);
                                        int fragmentIndex = rawurl.indexOf("#");
                                        if (fragmentIndex != -1) {
                                            rawurl = rawurl.substring(0, fragmentIndex);
                                        }
                                        if (rawurl.equals("")) {
                                            continue;
                                        }
                                        if (rawurl.startsWith("/")) {
                                            rawurl = beforepath + rawurl;
                                        } else {
                                            rawurl = beforepath + path + rawurl;
                                        }
                                        while (rawurl.contains("..")) {
                                            int index = rawurl.indexOf("..");
                                            int slashIndex = rawurl.lastIndexOf('/', index - 2);
                                            if (slashIndex != -1) {
                                                rawurl = rawurl.substring(0, slashIndex) + rawurl.substring(index + 2);
                                            } else {
                                                break;
                                            }
                                        }
                                        updatedResults.add(rawurl);
                                    }

                                    return updatedResults;
                                }
                            } else if (headResponseCode == 301 || headResponseCode == 302 || headResponseCode == 303
                                    || headResponseCode == 307 || headResponseCode == 308) {
                                // System.out.print("redirect"+urls+"to"+headResponseCode);
                                Row r = new Row(Hasher.hash(urls));
                                r.put("url", urls);
                                r.put("responseCode", "" + headResponseCode);
                                context.getKVS().putRow("pt-crawl", r);
                                String redirectUrl = headconnection.getHeaderField("Location");
                                results.add(redirectUrl);
                                System.out.println("redirect" + urls + "to" + headResponseCode + redirectUrl);
                                return results;
                            } else {
                                System.out.println("error" + urls + "to" + headResponseCode);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return results;
                    });
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public static List<String> extracturl(String URL) {
        List<String> extractedUrls = new ArrayList<>();

        // Convert the HTML to lowercase for case-insensitive matching
        // String lowerHtml = URL.toLowerCase();

        // Split the HTML by open angle brackets ("<")
        String[] tags = URL.split("<");

        for (String tag : tags) {
            // Remove any leading and trailing white spaces
            tag = tag.trim();

            // Check if it's not an empty string and starts with "a" (anchor tag)
            if (!tag.isEmpty() && tag.startsWith("a")) {
                // Find the "href" attribute
                int hrefIndex = tag.indexOf("href=\"");

                if (hrefIndex != -1) {
                    int urlStart = hrefIndex + 6; // Start of the URL
                    int urlEnd = tag.indexOf("\"", urlStart); // End of the URL
                    if (urlEnd != -1) {
                        String extractedUrl = tag.substring(urlStart, urlEnd);
                        extractedUrls.add(extractedUrl);
                    }
                }
            }
        }

        return extractedUrls;
    }

    public static String[] getrobotstxt(String URL) {
        try {
            URL urlOb = new URL(URL + "/robots.txt");
            HttpURLConnection connection = (HttpURLConnection) urlOb.openConnection();
            connection.setRequestMethod("GET");
            connection.connect();
            int responseCode = connection.getResponseCode();

            if (responseCode == 200) {
                // 读取 robots.txt 文件内容
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                StringBuilder robotstxtContent = new StringBuilder();
                String line;

                while ((line = reader.readLine()) != null) {
                    robotstxtContent.append(line).append("\n");
                }
                reader.close();

                // 解析 robots.txt 内容
                String[] lines = robotstxtContent.toString().split("\n");
                List<String> disallowRules = new ArrayList<>();
                List<String> allowRules = new ArrayList();
                String delay = "1";
                boolean specificRules = false; // 是否有特定的规则适用于 "cis5550-crawler" User-agent

                for (String robotstxtLine : lines) {
                    String trimmedLine = robotstxtLine.trim();

                    // 检查是否有特定User-agent规则
                    if (trimmedLine.equalsIgnoreCase("User-agent: cis5550-crawler")) {
                        specificRules = true;
                        continue;
                    } else if (specificRules && trimmedLine.startsWith("Disallow: ")) {
                        disallowRules.add(trimmedLine.substring("Disallow: ".length()));
                    } else if (specificRules && trimmedLine.startsWith("Allow: ")) {
                        allowRules.add(trimmedLine.substring("Allow: ".length()));
                    } else if (trimmedLine.equalsIgnoreCase("User-agent: *")) {
                        specificRules = false; // 恢复到通配符(*)规则
                    } else if (specificRules && trimmedLine.startsWith("Crawl-delay: ")) {
                        delay = (trimmedLine.substring("Crawl-delay: ".length()));
                    }
                }
                String disallowString = "";
                if (!disallowRules.isEmpty()) {
                    disallowString = String.join(",", disallowRules); // 没有 Disallow 规则
                }
                String allowString = "";
                if (!allowRules.isEmpty()) {
                    allowString = String.join(",", allowRules); // 没有 Disallow 规则
                }

                // 返回 Disallow 和 Allow 规则
                return new String[] { disallowString, allowString, delay };
            } else {
                return null; // 没有 robots.txt 文件
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    public static String gethost(String[] seedurl1) {
        String host = "";
        if (seedurl1[0] == null) {
            System.out.println("  ++++++++++ url missing host：" + seedurl1[1] + seedurl1[2] + seedurl1[3]);
        } else if (seedurl1[2] != null) {
            host = seedurl1[0] + "://" + seedurl1[1] + ":" + seedurl1[2];
        } else {
            if (seedurl1[0].equals("http")) {
                host = seedurl1[0] + "://" + seedurl1[1] + ":" + "80";
            } else {
                host = seedurl1[0] + "://" + seedurl1[1] + ":" + "8000";
            }
        }
        return host;
    }

    public static void addhostrecord(FlameContext context, String[] seedurl1) {
        String host = gethost(seedurl1);
        String[] rules = getrobotstxt(host);
        Row visitrow = new Row(Hasher.hash(seedurl1[1] + seedurl1[2]));
        visitrow.put("time", "" + System.currentTimeMillis());
        if (rules == null) {
            visitrow.put("disallow", "");
            visitrow.put("allow", "");
            visitrow.put("delay", "1");
        } else {
            visitrow.put("disallow", rules[0]);
            visitrow.put("allow", rules[1]);
            visitrow.put("delay", rules[2]);
        }
        try {
            context.getKVS().putRow("visit-history", visitrow);
        } catch (Exception e) {
            System.out.println(seedurl1[1] + seedurl1[2] + rules[0] + rules[1]);
            e.printStackTrace();
        }
    }
}
