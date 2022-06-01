package com.github.apaceh.model;

import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@AllArgsConstructor
@Slf4j
@Builder
@Data
public class User {
    private String id;
    private String firstName;
    private String lastName;
    private String userAddress;

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
