-- Add session_token (login requires it; see user_utils.create_session).
-- If NOT NULL fails on non-empty user_session, use: TRUNCATE TABLE user_session; then re-run.

ALTER TABLE `user_session`
  ADD COLUMN `session_token` varchar(64) NOT NULL AFTER `user_id`,
  ADD UNIQUE KEY `uq_user_session_token` (`session_token`);
