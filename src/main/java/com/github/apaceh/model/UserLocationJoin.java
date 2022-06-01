package com.github.apaceh.model;

import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@NoArgsConstructor
@AllArgsConstructor
@Slf4j
@Builder
@Data
public class UserLocationJoin {
    private String id;
    private String firstName;
    private String lastName;
    private String userAddress;
    private List<UserLocation> userLocations = new ArrayList<>();

    public void addLocation(final UserLocation addr) {
        if (Objects.isNull(addr)) {
            return;
        }
        for (int i=0; i< this.userLocations.size(); i++) {
            if (this.userLocations.get(i).getId().equals(addr.getId())) {
                this.userLocations.remove(i);
                break;
            }
        }
        this.userLocations.add(addr);
    }

    public static UserLocationJoin getInstance(final User user, final UserLocation userLocations) {
        return UserLocationJoin.builder()
                .id(user.getId())
                .firstName(user.getFirstName())
                .lastName(user.getLastName())
                .userAddress(user.getUserAddress())
                .userLocations(Arrays.asList(userLocations))
                .build();
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
