DROP TABLE IF EXISTS enrichment_test;

CREATE TABLE IF NOT EXISTS enrichment_test(
   city         VARCHAR(16)     NOT NULL
  ,date_time    VARCHAR(24)     NOT NULL
  ,name         VARCHAR(16)     NOT NULL
  ,speed        NUMERIC(4,1)    NOT NULL
  ,aux          VARCHAR(16)     NOT NULL
  ,country      VARCHAR(32)     NOT NULL
  ,pk           INTEGER         NOT NULL PRIMARY KEY
);

INSERT INTO enrichment_test(city,date_time,name,speed,aux,country,pk) VALUES ('New York','2016-02-07T10:10:00.000Z','eve',2.5,'ue_test_ny','USA',3);
INSERT INTO enrichment_test(city,date_time,name,speed,aux,country,pk) VALUES ('Krasnoyarsk','2016-01-07T10:10:34.000Z','alice',10.0,'ue_test_krsk','Russia',1);
INSERT INTO enrichment_test(city,date_time,name,speed,aux,country,pk) VALUES ('London','2016-01-08T10:00:34.000Z','bob',25.0,'ue_test_london','England',2);
INSERT INTO enrichment_test(city,date_time,name,speed,aux,country,pk) VALUES ('Delray Beach','2016-01-08T10:00:34.000Z','boris',25.0,'ue_test_delray','US',4);
