package com.github.apaceh.model;

import com.google.gson.Gson;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@AllArgsConstructor
@Slf4j
@Builder
@Data
@EqualsAndHashCode
public class UserLocation {
    private String id;
    private String lat;
    private String lng;
    private String userId;

    @Override
    public boolean equals(Object other) {
        return other instanceof UserLocation && ((UserLocation) other).getId().equals(this.getId());
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
