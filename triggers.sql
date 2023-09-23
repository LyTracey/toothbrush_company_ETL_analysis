-- Trigger to insert new row into toothbrush_types table if a new toothbrush_type appears

CREATE DEFINER=`admin`@`%` PROCEDURE `update_log`(IN id INT(2), IN status INT(2))
BEGIN
	UPDATE Download_to_s3_queue 
		SET 
			updated_at = NOW(), 
			status_id = status 
		WHERE file_id = id;
        
	SELECT process
		FROM Download_to_s3_queue
        JOIN Status
			ON Download_to_s3_queue.status_id = Status.status_id
        WHERE file_id = id;
		
END