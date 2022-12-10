package io.mykidong.iceberg.example.dataapi.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.util.concurrent.Callable;

public class ControllerUtils {

    private static Logger LOG = LoggerFactory.getLogger(ControllerUtils.class);

    public static String successMessage() {
        return "{ 'result': 'SUCCESS'}";
    }

    public static String doProcess(Callable<String> task) {
        try {
            return task.call();
        } catch (Exception e) {

            e.printStackTrace();
            LOG.info("instanceof " + e.getClass());

            if(e instanceof ResponseStatusException) {
                throw (ResponseStatusException) e;
            } else {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
            }
        }
    }
}
