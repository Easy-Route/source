package org.dataflow.bootstrap.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
public class SecurityConfig {

    @Bean
    public PasswordEncoder passwordEncoder() {
        return PasswordEncoderFactories.createDelegatingPasswordEncoder();
    }

    @Bean
    public InMemoryUserDetailsManager userDetailsService(
            PasswordEncoder encoder,
            @Value("${dataflow.security.admin.user:admin}") String adminUser,
            @Value("${dataflow.security.admin.password:admin}") String adminPassword,
            @Value("${dataflow.security.viewer.user:viewer}") String viewerUser,
            @Value("${dataflow.security.viewer.password:viewer}") String viewerPassword) {
        UserDetails admin = User.withUsername(adminUser)
                .password(encoder.encode(adminPassword))
                .roles("ADMIN", "VIEWER")
                .build();
        UserDetails viewer = User.withUsername(viewerUser)
                .password(encoder.encode(viewerPassword))
                .roles("VIEWER")
                .build();
        return new InMemoryUserDetailsManager(admin, viewer);
    }

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
                .csrf(csrf -> csrf.disable())
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/actuator/health", "/actuator/info", "/v3/api-docs/**", "/swagger-ui/**").permitAll()
                        .requestMatchers("/actuator/**").hasRole("ADMIN")
                        .requestMatchers("/api/v1/sources/**").hasAnyRole("ADMIN")
                        .requestMatchers("/api/v1/tracts/*/deploy").hasRole("ADMIN")
                        .requestMatchers("/api/v1/tracts/*/suspend").hasRole("ADMIN")
                        .requestMatchers("/api/v1/tracts/*/resume").hasRole("ADMIN")
                        .requestMatchers(org.springframework.http.HttpMethod.POST, "/api/v1/tracts/**").hasRole("ADMIN")
                        .requestMatchers(org.springframework.http.HttpMethod.PUT, "/api/v1/tracts/**").hasRole("ADMIN")
                        .requestMatchers(org.springframework.http.HttpMethod.DELETE, "/api/v1/tracts/**").hasRole("ADMIN")
                        .requestMatchers("/api/v1/tracts/**").hasAnyRole("ADMIN", "VIEWER")
                        .anyRequest().authenticated())
                .httpBasic(org.springframework.security.config.Customizer.withDefaults());
        return http.build();
    }
}
