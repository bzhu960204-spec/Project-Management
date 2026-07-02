package com.pm.logs;

import com.pm.process.ManagedProcess;
import com.pm.process.ProcessSupervisor;
import com.pm.project.Project;
import com.pm.project.ProjectRepository;
import com.pm.project.ProjectService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

@RestController
@RequestMapping("/api/projects/{id}/logs")
@RequiredArgsConstructor
public class LogsController {

    private final ProjectRepository projectRepo;
    private final ProcessSupervisor supervisor;

    @Value("${pm.logs.dir}")
    private String logsDir;

    @GetMapping
    public List<String> tail(@PathVariable String id,
                             @RequestParam(defaultValue = "500") int tail) {
        ManagedProcess mp = supervisor.getLive(id).orElse(null);
        if (mp == null) return Collections.emptyList();
        return mp.getLogs().tail(tail);
    }

    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(@PathVariable String id) {
        Project p = projectRepo.findById(id)
                .orElseThrow(() -> new ProjectService.NotFoundException("Project not found: " + id));
        SseEmitter emitter = new SseEmitter(0L); // no timeout
        ManagedProcess mp = supervisor.getLive(p.getId()).orElse(null);
        if (mp != null) {
            mp.subscribe(emitter);
            return emitter;
        }
        // No live ManagedProcess (e.g. backend was restarted and the project is now
        // ATTACHED, or the project is STOPPED/EXTERNAL). Fall back to streaming the
        // tail of the most recent on-disk log file so the user still sees content.
        streamLatestFileTail(emitter, p);
        return emitter;
    }

    /** Stream the last ~500 lines of the most recent .log file for this project, then close. */
    private void streamLatestFileTail(SseEmitter emitter, Project p) {
        Thread t = new Thread(() -> {
            try {
                Path latest = findLatestLogFile(p.getId());
                if (latest == null) {
                    emitter.send(SseEmitter.event().name("log")
                            .data("[pm] no live process and no archived logs for " + p.getName()));
                } else {
                    emitter.send(SseEmitter.event().name("log")
                            .data("[pm] no live process — showing tail of " + latest.getFileName()));
                    List<String> all = Files.readAllLines(latest, StandardCharsets.UTF_8);
                    int from = Math.max(0, all.size() - 500);
                    for (int i = from; i < all.size(); i++) {
                        emitter.send(SseEmitter.event().name("log").data(all.get(i)));
                    }
                    emitter.send(SseEmitter.event().name("log")
                            .data("[pm] (end of archived log — start the project to resume live streaming)"));
                }
                emitter.complete();
            } catch (Exception e) {
                try { emitter.completeWithError(e); } catch (Exception ignored) {}
            }
        }, "pm-log-tail-" + p.getId());
        t.setDaemon(true);
        t.start();
    }

    private Path findLatestLogFile(String projectId) {
        Path dir = Paths.get(logsDir);
        if (!Files.isDirectory(dir)) return null;
        String prefix = projectId + "-";
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(Files::isRegularFile)
                    .filter(pp -> {
                        String n = pp.getFileName().toString();
                        return n.startsWith(prefix) && n.endsWith(".log");
                    })
                    .max(Comparator.comparing(pp -> {
                        try { return Files.getLastModifiedTime(pp); }
                        catch (IOException e) { return FileTime.fromMillis(0); }
                    }))
                    .orElse(null);
        } catch (IOException e) {
            return null;
        }
    }

    /** List archived log files for this project, newest first. */
    @GetMapping("/history")
    public List<LogFileEntry> history(@PathVariable String id) {
        // Make sure the project exists (defensive; also normalises 404).
        projectRepo.findById(id)
                .orElseThrow(() -> new ProjectService.NotFoundException("Project not found: " + id));
        Path dir = Paths.get(logsDir);
        if (!Files.isDirectory(dir)) return List.of();
        String prefix = id + "-";
        try (Stream<Path> stream = Files.list(dir)) {
            return stream
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().startsWith(prefix)
                            && p.getFileName().toString().endsWith(".log"))
                    .map(LogsController::toEntry)
                    .filter(e -> e != null)
                    .sorted(Comparator.comparing((LogFileEntry e) -> e.modifiedAt).reversed())
                    .toList();
        } catch (IOException e) {
            return List.of();
        }
    }

    /** Download / view the contents of one archived log file (plain text). */
    @GetMapping("/history/{filename}")
    public ResponseEntity<String> historyFile(@PathVariable String id,
                                              @PathVariable String filename,
                                              @RequestParam(defaultValue = "false") boolean download) throws IOException {
        projectRepo.findById(id)
                .orElseThrow(() -> new ProjectService.NotFoundException("Project not found: " + id));
        // Whitelist: filename must match expected pattern and start with the project id.
        if (filename.contains("/") || filename.contains("\\") || filename.contains("..")) {
            return ResponseEntity.badRequest().body("Invalid filename");
        }
        if (!filename.startsWith(id + "-") || !filename.endsWith(".log")) {
            return ResponseEntity.badRequest().body("Filename does not belong to this project");
        }
        Path file = Paths.get(logsDir).resolve(filename).normalize();
        if (!file.startsWith(Paths.get(logsDir).toAbsolutePath().normalize())
                && !file.startsWith(Paths.get(logsDir).normalize())) {
            return ResponseEntity.badRequest().body("Path escape detected");
        }
        if (!Files.isRegularFile(file)) {
            return ResponseEntity.notFound().build();
        }
        String body = Files.readString(file);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        if (download) {
            headers.setContentDispositionFormData("attachment", filename);
        }
        return new ResponseEntity<>(body, headers, 200);
    }

    private static LogFileEntry toEntry(Path p) {
        try {
            LogFileEntry e = new LogFileEntry();
            e.filename = p.getFileName().toString();
            e.size = Files.size(p);
            e.modifiedAt = Files.getLastModifiedTime(p).toInstant();
            return e;
        } catch (IOException ex) {
            return null;
        }
    }

    public static class LogFileEntry {
        public String filename;
        public long size;
        public Instant modifiedAt;
    }
}
