package org.example.event;

public record UserEvent(UserOperation operation, String email) {
}
