package io.mykidong.iceberg.example.dataapi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import javax.servlet.http.HttpServletRequest;

public abstract class AbstractController{

    private static Logger LOG = LoggerFactory.getLogger(AbstractController.class);

    protected ObjectMapper mapper = new ObjectMapper();

    @Autowired
    protected Environment env;

    @Autowired
    protected HttpServletRequest context;
}
