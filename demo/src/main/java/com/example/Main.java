package com.example;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Main {
    static ObjectMapper mapper = new ObjectMapper();
    static Path incomingPath = Path.of("demo/src/main/resources/data/incoming");
    static Path processedPath = Path.of("demo/src/main/resources/data/processed");

    public static void main(String[] args) throws Exception {
        if (!Files.exists(incomingPath))
            Files.createDirectories(incomingPath);
        if (!Files.exists(processedPath))
            Files.createDirectories(processedPath);

        List<Path> jsonFiles = Files.list(incomingPath).filter(p -> p.toString().endsWith(".json")).toList();
        processJsonFiles(jsonFiles, processedPath);

        readFileWithBuffer(incomingPath.resolve("1.json"));

        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            incomingPath.register(
                watchService,
                StandardWatchEventKinds.ENTRY_CREATE
            );
            System.out.println("Директория наблюдается...");

            WatchKey key;
            while(true) {
                try {
                    System.out.println("Новый файл ожидается...");
                    key = watchService.take();
                } catch (InterruptedException e) {
                    break;
                }

                for (WatchEvent<?> event: key.pollEvents()) {
                    processJsonFiles(List.of(
                        Path.of(incomingPath.toString(), ((Path) event.context()).toString())
                    ), processedPath);
                }
                boolean valid = key.reset();
                if (!valid) {
                    break;
                }
            }

        } catch (Exception e) {
            System.out.println("Ошибка ресурса WatchService: " + e);
        }
    }

    public static void processJsonFiles(List<Path> jsonFiles, Path processedPath) {
        for (Path jsonFile : jsonFiles) {
            try (FileChannel inChannel = FileChannel.open(jsonFile, StandardOpenOption.READ)) {
                InputStream is = Channels.newInputStream(inChannel);
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                DigestInputStream digestInputStream = new DigestInputStream(is, digest);
                JsonNode json = mapper.readTree(digestInputStream);
                digestInputStream.close();

                if (json.isObject()) {
                    ((ObjectNode) json).put("processed", true);
                }
                Path processedFile = processedPath.resolve(jsonFile.getFileName());
                
                byte[] processedJsonBytes = mapper.writeValueAsBytes(json);
                ByteBuffer buffer = ByteBuffer.wrap(processedJsonBytes);
                
                try (FileChannel outChannel = FileChannel.open(processedFile, StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                    while (buffer.hasRemaining()) {
                        outChannel.write(buffer);
                    }
                    System.out.println(jsonFile.getFileName());
                    System.out.println("Размер: " + outChannel.size() + " байтов");
                    System.out.println("SHA-256: " + HexFormat.of().formatHex(digest.digest()));
                    System.out.println("Статус: обработан");
                } catch (Exception e) {
                    System.out.println("Ошибка записи обработанного файла: " + e);
                }
            } catch (Exception e) {
                System.out.println("Ошибка чтения файла: " + e);
            }
        }
    }

    public static void readFileWithBuffer(Path filePath) {
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            while (channel.read(buffer) != -1) {
                buffer.flip();
                System.out.println(StandardCharsets.UTF_8.decode(buffer));
                buffer.clear();
            }
        } catch (Exception e) {
            System.out.println("Ошибка чтения файла: " + e);
        }
    }
}