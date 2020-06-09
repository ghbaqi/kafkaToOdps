package com.hupu.hermes.sink;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;


// curl 192.168.6.250:13579/hi
@RestController
public class TestController {

    int i = 0;

    @GetMapping("/hi")
    public String hi() {
        return "hi " + i++;
    }

}
