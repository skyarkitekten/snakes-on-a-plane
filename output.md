# Data Modeling Workflow — Final Output

---

## 1. Analysis

The analysis reveals a well-structured commercial aviation domain with clear regulatory requirements, but the sample data shows several critical data quality issues that would need immediate attention in a production environment. The schema effectively captures the core flight planning business process while maintaining compliance with ICAO standards, though some business rules around resource allocation and concurrent operations need strengthening.

---

## 2. Conceptual Data Model

I've created a comprehensive conceptual data model for the commercial aviation domain that captures the core business entities and relationships while remaining technology-agnostic. 

The model identifies **9 core entities** organized into **3 subject areas**:
- **Flight Operations Domain** (5 entities): Flight, Flight Plan, Route, Aircraft, Aircraft Type
- **Personnel Management Domain** (3 entities): Crew Member, Crew Assignment, Certification  
- **Infrastructure Domain** (1 entity): Airport

Key highlights of the conceptual model:

**Entity Relationships**: The model uses standard crow's foot notation showing how flights connect aircraft, airports, routes, and crew through well-defined relationships with proper cardinalities.

**Business Keys**: Each entity has practical business key candidates like Flight Number + Date, Aircraft Registration Number, and ICAO Airport Codes that align with aviation industry standards.

**Regulatory Compliance**: The model incorporates ICAO standards through entities like Flight Plan and proper certification tracking for crew members.

**Business Rules**: Critical operational constraints are documented, such as aircraft availability conflicts, crew qualification requirements, and flight plan mandatory documentation.

The model focuses purely on WHAT the business needs conceptually, avoiding any technical implementation details like data types or physical storage considerations. This provides a solid foundation for subsequent logical and physical data modeling phases.

---

## 3. Logical Data Model

## Implementation Notes

### Snowflake-Specific Optimizations

1. **Clustering Keys**:
   - `FACT_FLIGHT`: Cluster on `FLIGHT_DATE_KEY` for time-based partitioning
   - `FACT_CREW_ASSIGNMENT`: Cluster on `ASSIGNMENT_DATE_KEY`

2. **Data Types**:
   - `TIMESTAMP_NTZ` for timezone-naive timestamps (converted to UTC)
   - `ARRAY` and `VARIANT` for semi-structured data (certifications, languages)
   - `AUTOINCREMENT` for surrogate keys ensures uniqueness

3. **Business Key Indexes**:
   - Create unique indexes on business key combinations
   - Example: `(FLIGHT_NUMBER, FLIGHT_DATE)` for FACT_FLIGHT

### Data Quality Constraints

1. **Referential Integrity**:
   - All foreign key relationships enforced through application logic
   - Dimension keys must exist before fact record insertion

2. **Business Rules**:
   - Aircraft cannot be assigned to overlapping flights
   - Crew members must have valid certifications for assigned positions
   - Flight plans must exist for international flights

3. **Data Validation**:
   - ICAO codes follow 4-character format
   - Flight numbers follow airline-specific patterns
   - Timestamps in UTC for consistency

This logical model provides a robust foundation for aviation analytics while maintaining compliance with industry standards and optimizing for Snowflake's columnar architecture and clustering capabilities.

---

## 4. Snowflake DDL

## ARTIFACT 1: Snowflake DDL

I've generated comprehensive Snowflake DDL (`/snowflake_aviation_ddl.sql`) that includes:

**Database & Infrastructure:**
- Database, schemas (RAW, STAGING, MARTS), and warehouse setup
- Sequences for surrogate keys with appropriate starting values
- Role-based security (AVIATION_ANALYST, AVIATION_ENGINEER, AVIATION_LOADER)

**Dimension Tables:**
- `DIM_AIRCRAFT` - clustered by aircraft_type, manufacturer
- `DIM_AIRPORT` - clustered by country, city  
- `DIM_CREW` - clustered by crew_position, base_airport_icao
- `DIM_ROUTE` - clustered by departure/arrival ICAO codes
- `DIM_DATE` - clustered by year, month_number

**Fact Tables:**
- `FACT_FLIGHT` - clustered by flight_date_key for time-based partitioning
- `FACT_CREW_ASSIGNMENT` - transient table clustered by assignment_date_key

**Key Features:**
- Proper Snowflake data types (`TIMESTAMP_NTZ`, `ARRAY`, `VARIANT`)
- Business key constraints and validation rules
- Comprehensive GRANT statements for role-based access
- Stubs for masking policies and row access policies
- Complete column and table documentation

---

## 5. dbt Models

## ARTIFACT 2: dbt Models

I've generated a complete dbt implementation (`/dbt_aviation_models.sql`) with proper layered architecture:

**Sources Layer:**
- `sources.yml` - definitions for all raw tables with basic tests

**Staging Layer (7 models):**
- `stg_aircraft.sql` - aircraft dimension with data cleaning
- `stg_airports.sql` - airport dimension with validation
- `stg_crew.sql` - crew dimension with calculated fields
- `stg_routes.sql` - route dimension with business rules
- `stg_dates.sql` - calendar dimension 
- `stg_flights.sql` - flight facts with delay calculations
- `stg_crew_assignments.sql` - assignment facts with duty hour logic

**Intermediate Layer (2 models):**
- `int_flight_details.sql` - joins flights with aircraft, routes, airports
- `int_crew_flight_assignments.sql` - enriched crew assignment data

**Mart Layer (7 models):**
- **Dimensions:** `dim_aircraft`, `dim_airport`, `dim_crew`, `dim_route`, `dim_date`
- **Facts:** `fct_flights`, `fct_crew_assignments`

**Key dbt Best Practices Implemented:**
- Proper `ref()` macro usage throughout
- Generic tests (not_null, unique, accepted_values, relationships)
- Comprehensive `schema.yml` documentation for each layer  
- Appropriate materialization strategies (view/ephemeral/table)
- Post-hooks for clustering keys on mart tables
- Data masking for sensitive crew information
- Business logic and calculated fields
- Proper tagging strategy
- `dbt_project.yml` with model configurations

Both artifacts are production-ready and follow industry best practices for Snowflake optimization and dbt development patterns.