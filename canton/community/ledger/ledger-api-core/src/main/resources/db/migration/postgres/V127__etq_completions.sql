DROP INDEX participant_command_completion_offset_application_idx;
CREATE INDEX participant_command_completions_application_id_offset_idx ON participant_command_completions USING btree (application_id, completion_offset);
