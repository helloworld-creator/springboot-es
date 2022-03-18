package com.example.springesjd.controller;

import com.example.springesjd.service.ContentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
public class ContentController {

    @Autowired
    ContentService contentService;

    @GetMapping("/parse/{keywords}")  // http://localhost/parse/kerwords
    public boolean parse(@PathVariable("keywords") String keywords) throws IOException {
        return contentService.parseContent(keywords);
    }

    @GetMapping("/search/{keywords}/{pageOn}/{pageSize}")
    public List<Map<String,Object>> search(@PathVariable("keywords") String keywords,
                                           @PathVariable("pageOn") int pageOn,
                                           @PathVariable("pageSize") int pageSize) throws IOException {
        return contentService.searchContent(keywords,pageOn,pageSize);
    }

}

