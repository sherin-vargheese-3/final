package com.example.demo.service;

import com.example.demo.contract.UserDTO;
import com.example.demo.model.User;
import com.example.demo.repository.UserRepository;
import lombok.AllArgsConstructor;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
@AllArgsConstructor
public class UserService {

    private final UserRepository userRepository;
    private RedisTemplate<String, UserDTO> userRedisTemplate;
    private final MessageService messageService;

//    @Cacheable(value = "users", key = "#id")
//    public UserDTO getUserById(Long id) {
//        try {
//            Thread.sleep(3000);      //Delay for 3 second
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//        }
//        User user = userRepository.findById(id)
//                .orElseThrow(() -> new RuntimeException("User not found."));
//        return convertToDTO(user);
//    }

    public UserDTO getUserById(Long id) {
        String key = "users::" + id;
        ValueOperations<String, UserDTO> valueOps = userRedisTemplate.opsForValue();

        // Try fetching from Redis
        UserDTO cached = valueOps.get(key);
        if (cached != null) {
            return cached;
        }

        // Simulate delay for 3 seconds and load from DB and cache
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        User user = userRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("User not found"));
        UserDTO dto = convertToDTO(user);
        valueOps.set(key, dto, 30, TimeUnit.MINUTES);
        return dto;
    }

    @Cacheable(value = "allUsers")
    public List<UserDTO> getAllUsers() {
        try {
            Thread.sleep(3000);      //Delay for 3 second
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return userRepository.findAll()
                .stream()
                .map(this::convertToDTO)
                .collect(Collectors.toList());
    }

    @CacheEvict(value = {"users", "allUsers"}, allEntries = true)
    public UserDTO createUser(UserDTO userDTO) {
        User user = convertToEntity(userDTO);
        return convertToDTO(userRepository.save(user));
    }

    @CacheEvict(value = {"users", "allUsers"}, allEntries = true)
    public UserDTO updateUser(Long id, UserDTO userDTO) {
        User existingUser = userRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("User not found."));

        existingUser.setName(userDTO.getName());
        existingUser.setEmail(userDTO.getEmail());
        User updatedUser = userRepository.save(existingUser);

        messageService.processUserMessage("user updated");       // Integrating kafka
        return convertToDTO(updatedUser);
    }

    @CacheEvict(value = {"users", "allUsers"}, allEntries = true)
    public void deleteUser(Long id) {
        if (userRepository.existsById(id)) {
            userRepository.deleteById(id);
        } else {
            throw new RuntimeException("User not found.");
        }
    }

    private UserDTO convertToDTO(User user) {
        return UserDTO.builder()
                .id(user.getId())
                .name(user.getName())
                .email(user.getEmail())
                .build();
    }

    private User convertToEntity(UserDTO userDTO) {
        return User.builder()
                .id(userDTO.getId())
                .name(userDTO.getName())
                .email(userDTO.getEmail())
                .build();
    }
}
