package org.example.notification.dto;

public record UserEventMessage(UserOperation operation, String email) {
}
