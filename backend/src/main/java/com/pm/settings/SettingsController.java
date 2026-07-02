package com.pm.settings;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/settings")
public class SettingsController {

    private final AppSettingsRepository repo;

    public SettingsController(AppSettingsRepository repo) {
        this.repo = repo;
    }

    @GetMapping
    public AppSettingsDto get() {
        return repo.findById(1)
                .map(s -> new AppSettingsDto(s.getJavaHome()))
                .orElse(new AppSettingsDto(null));
    }

    @PutMapping
    public AppSettingsDto put(@RequestBody AppSettingsDto dto) {
        AppSettings s = repo.findById(1).orElseGet(AppSettings::new);
        String jh = dto.javaHome();
        s.setJavaHome(jh == null || jh.isBlank() ? null : jh.trim());
        return new AppSettingsDto(repo.save(s).getJavaHome());
    }
}
