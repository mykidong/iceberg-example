package io.mykidong.iceberg.example.dataapi.util;

import org.apache.commons.io.IOUtils;
import org.springframework.core.io.ClassPathResource;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class StringUtils {

    public static InputStream readFile(String filePath) {
        try {
            return new FileInputStream(filePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static InputStream readFileFromClasspath(String filePath) {
        try {
            return new ClassPathResource(filePath).getInputStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String fileToString(String filePath, boolean fromClasspath) {
        try {
            return fromClasspath ? IOUtils.toString(readFileFromClasspath(filePath)) :
                    IOUtils.toString(readFile(filePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getEnv(String key) {
        Map<String, String> envMap = System.getenv();
        return envMap.get(key);
    }
}
