-- SIPAE Health Reports tables
-- Run this manually in the SIPAE database configured by DB_SIPAE_*.

CREATE TABLE IF NOT EXISTS health_report_runs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_type VARCHAR(32) NOT NULL,
    report_date DATE NOT NULL,
    scheduled_for DATETIME NULL,
    started_at DATETIME NOT NULL,
    finished_at DATETIME NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'running',
    plantels_total INT NOT NULL DEFAULT 0,
    messages_generated INT NOT NULL DEFAULT 0,
    messages_sent INT NOT NULL DEFAULT 0,
    messages_failed INT NOT NULL DEFAULT 0,
    error TEXT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_health_report_runs_date (report_date),
    INDEX idx_health_report_runs_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS health_report_messages (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id BIGINT NULL,
    report_date DATE NOT NULL,
    plantel_code VARCHAR(16) NOT NULL,
    resolved_name VARCHAR(255) NULL,
    principal_email VARCHAR(255) NOT NULL,
    manager_email VARCHAR(255) NULL,
    cc_emails TEXT NULL,
    subject VARCHAR(500) NOT NULL,
    rfc_message_id VARCHAR(255) NOT NULL,
    gmail_message_id VARCHAR(255) NULL,
    gmail_thread_id VARCHAR(255) NULL,
    html_body LONGTEXT NOT NULL,
    text_summary TEXT NULL,
    model_json LONGTEXT NULL,
    worst_metric VARCHAR(64) NULL,
    severity VARCHAR(32) NOT NULL DEFAULT 'fulfilled',
    status VARCHAR(32) NOT NULL DEFAULT 'generated',
    open_token VARCHAR(128) NOT NULL,
    click_token VARCHAR(128) NOT NULL,
    generated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sent_at DATETIME NULL,
    failed_at DATETIME NULL,
    error TEXT NULL,
    open_count INT NOT NULL DEFAULT 0,
    first_opened_at DATETIME NULL,
    click_count INT NOT NULL DEFAULT 0,
    first_clicked_at DATETIME NULL,
    UNIQUE KEY uq_health_report_message_rfc (rfc_message_id),
    INDEX idx_health_report_messages_date_plantel (report_date, plantel_code),
    INDEX idx_health_report_messages_status (status),
    INDEX idx_health_report_messages_principal (principal_email),
    INDEX idx_health_report_messages_tokens (open_token, click_token),
    CONSTRAINT fk_health_report_messages_run FOREIGN KEY (run_id) REFERENCES health_report_runs(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS health_report_recipient_statuses (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    message_id BIGINT NOT NULL,
    recipient_email VARCHAR(255) NOT NULL,
    recipient_role VARCHAR(32) NOT NULL,
    delivery_status VARCHAR(32) NOT NULL DEFAULT 'pending',
    gmail_found_at DATETIME NULL,
    gmail_unread TINYINT(1) NULL,
    gmail_read_at DATETIME NULL,
    opened_at DATETIME NULL,
    clicked_at DATETIME NULL,
    last_checked_at DATETIME NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_health_report_recipient (message_id, recipient_email),
    INDEX idx_health_report_recipient_email (recipient_email),
    CONSTRAINT fk_health_report_recipient_message FOREIGN KEY (message_id) REFERENCES health_report_messages(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS health_report_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    message_id BIGINT NOT NULL,
    recipient_email VARCHAR(255) NULL,
    event_type VARCHAR(32) NOT NULL,
    event_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ip_hash VARCHAR(128) NULL,
    user_agent TEXT NULL,
    target_url TEXT NULL,
    INDEX idx_health_report_events_message (message_id),
    CONSTRAINT fk_health_report_events_message FOREIGN KEY (message_id) REFERENCES health_report_messages(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
