-- incase the schema exits already
DROP SCHEMA IF EXISTS crime_db CASCADE;

-- new schema for the crime database
CREATE SCHEMA crime_db;

-- ==============================================
-- Table: crime_type
-- Description: Stores various crime types (for use with both datasets)
-- ==============================================
CREATE TABLE crime_db.crime_type (
    crime_type_id SERIAL PRIMARY KEY,
    crime_type VARCHAR(255) NOT NULL,
    subtype_of_crime VARCHAR(255),
    modality VARCHAR(255)
);

-- ==============================================
-- Table: mexico_crime
-- Description: Stores aggregated crime statistics for Mexico.
-- ==============================================
CREATE TABLE crime_db.mexico_crime (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    entity_code INTEGER NOT NULL,
    entity VARCHAR(255) NOT NULL,
    affected_legal_good VARCHAR(255),
    crime_type_id INTEGER NOT NULL,
    count INTEGER,
    FOREIGN KEY (crime_type_id) REFERENCES crime_db.crime_type(crime_type_id)
);

-- ==============================================
-- Table: chicago_crime
-- Description: Stores detailed crime incident records for Chicago.
-- ==============================================
CREATE TABLE chicago_crime (
    id BIGSERIAL PRIMARY KEY,
    case_number VARCHAR(50) NOT NULL,        
    date TIMESTAMP NOT NULL,                 -- Date and time of the crime
    block VARCHAR(50),                       
    iucr INTEGER,                            
    primary_type VARCHAR(100),               -- Crime category 
    description VARCHAR(255),               
    location_description VARCHAR(255),       
    arrest BOOLEAN,                          --  police make an arrest? (TRUE/FALSE)
    domestic BOOLEAN,                        -- domestic incident? (TRUE/FALSE)
    beat VARCHAR(10),                        
    district INTEGER,                        
    ward INTEGER,                           
    community_area INTEGER,                  
    fbi_code INTEGER,                       
    x_coordinate DECIMAL,                  
    y_coordinate DECIMAL,                  
    year INTEGER,                          
    updated_on TIMESTAMP,                    
    latitude DECIMAL,                    
    longitude DECIMAL,                      
    location POINT                          -- (latitude, longitude)
);



-- ==============================================
-- Aggregated Views
-- ==============================================
-- Aggregated yearly crime count for Mexico
CREATE VIEW crime_db.mexico_aggregated_yearly AS
SELECT year, crime_type_id, SUM(count) AS total_crimes
FROM crime_db.mexico_crime
GROUP BY year, crime_type_id;

-- Aggregated yearly crime count for Chicago
CREATE VIEW crime_db.chicago_aggregated_yearly AS
SELECT year, crime_type_id, COUNT(*) AS total_crimes
FROM crime_db.chicago_crime
GROUP BY year, crime_type_id;

-- ==============================================
-- Indexes to optimize common queries
-- ==============================================
CREATE INDEX idx_mexico_year ON crime_db.mexico_crime(year);
CREATE INDEX idx_mexico_entity ON crime_db.mexico_crime(entity);
CREATE INDEX idx_chicago_year ON crime_db.chicago_crime(year);
CREATE INDEX idx_chicago_case_number ON crime_db.chicago_crime(case_number);
CREATE INDEX idx_chicago_primary_type ON crime_db.chicago_crime(crime_type_id);

