-- Create Download_to_s3_queue table
CREATE DEFINER=`admin`@`%` PROCEDURE `create_queue`()
BEGIN
	CREATE TABLE IF NOT EXISTS Download_to_s3_queue (
	file_id INT(2) UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    file_name VARCHAR(50) UNIQUE,
    created_at DATETIME,
    updated_at DATETIME,
    status_id INT(2) UNSIGNED REFERENCES Status(status_id)
);
END

-- Create Orders table
CREATE DEFINER=`admin`@`%` PROCEDURE `create_table`()
BEGIN
	CREATE TABLE IF NOT EXISTS Orders (
		`Order Number` VARCHAR(15) PRIMARY KEY,
		`Toothbrush Type` VARCHAR(30),
		`Order Date` DATETIME,
		`Customer Age` TINYINT,
		`Order Quantity` SMALLINT,
		`Delivery Postcode` VARCHAR(15),
		`Billing Postcode` VARCHAR(15),
		`is_first` BOOLEAN,
		`Dispatch Status` VARCHAR(15),
		`Dispatched Date` DATETIME,
		`Delivery Status` VARCHAR(15),
		`Delivery Date` DATETIME
	);
END

-- Get columns
CREATE DEFINER=`admin`@`%` PROCEDURE `get_cols`(IN tn VARCHAR(20))
BEGIN
	SELECT COLUMN_NAME 
	FROM INFORMATION_SCHEMA.COLUMNS
	WHERE TABLE_SCHEMA = "my-schema"
		AND TABLE_NAME = tn;
END

-- Insert log into Download_to_s3_queue
CREATE DEFINER=`admin`@`%` PROCEDURE `insert_log`(IN name VARCHAR(50), IN status INT(2))
BEGIN

	INSERT IGNORE INTO Download_to_s3_queue (file_name, created_at, updated_at, status_id) 
    VALUES (name, NOW(), NOW(), status);

	SELECT file_id
		FROM Download_to_s3_queue
		WHERE file_name = name;
END

-- Insert row into orders table

CREATE DEFINER=`admin`@`%` PROCEDURE `insert_row`(
	IN number VARCHAR(50), 
	IN type VARCHAR(30),
    IN date DATETIME,
    IN age INT(3),
    IN quantity INT(3) UNSIGNED,
    IN delivery VARCHAR(20),
    IN billing VARCHAR(20),
    IN first INT(1) UNSIGNED,
    IN dis_status VARCHAR(30),
    IN dis_date DATETIME,
    IN del_status VARCHAR(30),
    IN del_date DATETIME
)
BEGIN
	INSERT INTO Orders (
		`Order Number`,
        `Toothbrush Type`,
        `Order Date`,
        `Customer Age`,
        `Order Quantity`,
        `Delivery Postcode`,
		`Billing Postcode`,
        `is_first`,
        `Dispatch Status`,
        `Dispatched Date`,
        `Delivery Status`,
        `Delivery Date`
	)
		VALUES (
			number,
            type,
            date,
            age,
            quantity,
            delivery,
            billing,
            first,
            dis_status,
            dis_date,
            del_status,
            del_date
        );
END

-- Update status of log in Download_to_s3_queue

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