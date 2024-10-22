CREATE TABLE `items` (
    `id`         INTEGER AUTO_INCREMENT NOT NULL,
    `uuid`       VARCHAR(36)            NOT NULL UNIQUE,
    `owner_id`   VARCHAR(36)            NOT NULL,
    `owner_name` VARCHAR(255),
    `buyer_id`   VARCHAR(36),
    `item`       CLOB                   NOT NULL,
    `category`   VARCHAR(255),
    `price`      DOUBLE PRECISION,
    `tax`        DOUBLE PRECISION,
    `time`       BIGINT                 NOT NULL,
    `biddable`   BOOLEAN,
    `bids`       CLOB,
    `update`     BIGINT,
    `collected`  BOOLEAN
);

CREATE INDEX `expired_items` ON `items` (`buyer_id`, `time`, `collected`);

CREATE INDEX `collection_box` ON `items` (`buyer_id`, `collected`);

CREATE INDEX `history` ON `items` (`owner_id`, `buyer_id`);