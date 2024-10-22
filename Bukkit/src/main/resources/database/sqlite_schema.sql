CREATE TABLE `items` (
    `id`         INTEGER PRIMARY KEY NOT NULL,
    `uuid`       VARCHAR(36)         NOT NULL UNIQUE,
    `owner_id`   VARCHAR(36)         NOT NULL,
    `owner_name` VARCHAR(255),
    `buyer_id`   VARCHAR(36),
    `item`       TEXT                NOT NULL,
    `category`   VARCHAR(255),
    `price`      DOUBLE,
    `tax`        DOUBLE,
    `time`       BIGINT              NOT NULL,
    `biddable`   BOOLEAN,
    `bids`       TEXT,
    `update`     BIGINT,
    `collected`  BOOLEAN,
    PRIMARY KEY (`id`)
);

CREATE INDEX `expired_items` ON `items` (`buyer_id`, `time`, `collected`);

CREATE INDEX `collection_box` ON `items` (`buyer_id`, `collected`);

CREATE INDEX `history` ON `items` (`owner_id`, `buyer_id`);