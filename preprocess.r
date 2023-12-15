library(tidyverse)
library(bigrquery)

# This query represents dataset "case data" for domain "survey" and was generated for All of Us Registered Tier Dataset v7
dataset_46344540_survey_sql <- paste("
    SELECT
        answer.person_id,
        answer.survey_datetime,
        answer.question,
        answer.answer  
    FROM
        `ds_survey` answer   
    WHERE
        (
            question_concept_id IN (
                1585741, 1585873, 1586198
            )
        )  
        AND (
            answer.PERSON_ID IN (
                SELECT
                    distinct person_id  
                FROM
                    `cb_search_person` cb_search_person  
                WHERE
                    cb_search_person.person_id IN (
                        SELECT
                            criteria.person_id 
                        FROM
                            (SELECT
                                DISTINCT person_id,
                                entry_date,
                                concept_id 
                            FROM
                                `cb_search_all_events` 
                            WHERE
                                person_id IN (
                                    SELECT
                                        person_id 
                                    FROM
                                        `cb_search_all_events` 
                                    WHERE
                                        concept_id IN (
                                            SELECT
                                                DISTINCT c.concept_id 
                                            FROM
                                                `cb_criteria` c 
                                            JOIN
                                                (
                                                    select
                                                        cast(cr.id as string) as id 
                                                    FROM
                                                        `cb_criteria` cr 
                                                    WHERE
                                                        concept_id IN (317309) 
                                                        AND full_text LIKE '%_rank1]%'
                                                ) a 
                                                    ON (
                                                        c.path LIKE CONCAT('%.',
                                                    a.id,
                                                    '.%') 
                                                    OR c.path LIKE CONCAT('%.',
                                                    a.id) 
                                                    OR c.path LIKE CONCAT(a.id,
                                                    '.%') 
                                                    OR c.path = a.id) 
                                                WHERE
                                                    is_standard = 1 
                                                    AND is_selectable = 1
                                                ) 
                                                AND is_standard = 1 
                                            UNION
                                            ALL SELECT
                                                person_id 
                                            FROM
                                                `cb_search_all_events` 
                                            WHERE
                                                concept_id IN (
                                                    SELECT
                                                        DISTINCT c.concept_id 
                                                    FROM
                                                        `cb_criteria` c 
                                                    JOIN
                                                        (
                                                            select
                                                                cast(cr.id as string) as id 
                                                            FROM
                                                                `cb_criteria` cr 
                                                            WHERE
                                                                concept_id IN (1569324) 
                                                                AND full_text LIKE '%_rank1]%'
                                                        ) a 
                                                            ON (
                                                                c.path LIKE CONCAT('%.',
                                                            a.id,
                                                            '.%') 
                                                            OR c.path LIKE CONCAT('%.',
                                                            a.id) 
                                                            OR c.path LIKE CONCAT(a.id,
                                                            '.%') 
                                                            OR c.path = a.id) 
                                                        WHERE
                                                            is_standard = 0 
                                                            AND is_selectable = 1
                                                        ) 
                                                        AND is_standard = 0 
                                                )
                                            ) criteria 
                                    ) 
                                    AND cb_search_person.person_id IN (
                                        SELECT
                                            person_id 
                                        FROM
                                            `cb_search_person` p 
                                        WHERE
                                            age_at_cdr BETWEEN 40 AND 120 
                                            AND NOT EXISTS (
                                                SELECT
                                                    'x' 
                                                FROM
                                                    `death` d 
                                                WHERE
                                                    d.person_id = p.person_id
                                            ) 
                                        ) 
                                        AND cb_search_person.person_id IN (
                                            SELECT
                                                person_id 
                                            FROM
                                                `cb_search_person` p 
                                            WHERE
                                                has_ehr_data = 1 
                                        ) 
                                    )
                            )", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
survey_46344540_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "survey_46344540",
  "survey_46344540_*.csv")
message(str_glue('The data will be written to {survey_46344540_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_46344540_survey_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  survey_46344540_path,
  destination_format = "CSV")



# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {survey_46344540_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- cols(survey = col_character(), question = col_character(), answer = col_character())
  bind_rows(
    map(system2('gsutil', args = c('ls', export_path), stdout = TRUE, stderr = TRUE),
        function(csv) {
          message(str_glue('Loading {csv}.'))
          chunk <- read_csv(pipe(str_glue('gsutil cat {csv}')), col_types = col_types, show_col_types = FALSE)
          if (is.null(col_types)) {
            col_types <- spec(chunk)
          }
          chunk
        }))
}
dataset_46344540_survey_df <- read_bq_export_from_workspace_bucket(survey_46344540_path)

dim(dataset_46344540_survey_df)

head(dataset_46344540_survey_df, 5)

library(tidyverse)
library(bigrquery)

# This query represents dataset "case data" for domain "condition" and was generated for All of Us Registered Tier Dataset v7
dataset_46344540_condition_sql <- paste("
    SELECT
        c_occurrence.person_id,
        c_occurrence.condition_concept_id,
        c_standard_concept.concept_name as standard_concept_name,
        c_standard_concept.concept_code as standard_concept_code,
        c_occurrence.condition_start_datetime,
        c_occurrence.condition_source_value,
        c_source_concept.concept_name as source_concept_name,
        c_source_concept.concept_code as source_concept_code 
    FROM
        ( SELECT
            * 
        FROM
            `condition_occurrence` c_occurrence 
        WHERE
            (
                condition_concept_id IN  (
                    SELECT
                        DISTINCT c.concept_id 
                    FROM
                        `cb_criteria` c 
                    JOIN
                        (
                            select
                                cast(cr.id as string) as id 
                            FROM
                                `cb_criteria` cr 
                            WHERE
                                concept_id IN (
                                    201820, 316866, 4024561, 432867
                                ) 
                                AND full_text LIKE '%_rank1]%'
                        ) a 
                            ON (
                                c.path LIKE CONCAT('%.',
                            a.id,
                            '.%') 
                            OR c.path LIKE CONCAT('%.',
                            a.id) 
                            OR c.path LIKE CONCAT(a.id,
                            '.%') 
                            OR c.path = a.id) 
                        WHERE
                            is_standard = 1 
                            AND is_selectable = 1
                        ) 
                        OR  condition_source_concept_id IN  (
                            SELECT
                                DISTINCT c.concept_id 
                            FROM
                                `cb_criteria` c 
                            JOIN
                                (
                                    select
                                        cast(cr.id as string) as id 
                                    FROM
                                        `cb_criteria` cr 
                                    WHERE
                                        concept_id IN (
                                            35207668, 44819500, 44819501, 44819502, 44819503, 44819504, 44820682, 44820683, 44820684, 44820685, 44821787, 44822934, 44822935, 44822936, 44824071, 44824072, 44824073, 44824074, 44825264, 44826459, 44826460, 44826461, 44827615, 44827616, 44827617, 44828793, 44828794, 44828795, 44829878, 44829879, 44829880, 44829881, 44829882, 44831045, 44831046, 44831047, 44832190, 44832191, 44832192, 44832193, 44832194, 44833365, 44833366, 44833367, 44833368, 44834548, 44834549, 44836914, 44836915, 44836916, 44836917, 44836918
                                        ) 
                                        AND full_text LIKE '%_rank1]%'
                                ) a 
                                    ON (
                                        c.path LIKE CONCAT('%.',
                                    a.id,
                                    '.%') 
                                    OR c.path LIKE CONCAT('%.',
                                    a.id) 
                                    OR c.path LIKE CONCAT(a.id,
                                    '.%') 
                                    OR c.path = a.id) 
                                WHERE
                                    is_standard = 0 
                                    AND is_selectable = 1
                                )
                        )  
                        AND (
                            c_occurrence.PERSON_ID IN (
                                SELECT
                                    distinct person_id  
                                FROM
                                    `cb_search_person` cb_search_person  
                                WHERE
                                    cb_search_person.person_id IN (
                                        SELECT
                                            criteria.person_id 
                                        FROM
                                            (SELECT
                                                DISTINCT person_id,
                                                entry_date,
                                                concept_id 
                                            FROM
                                                `cb_search_all_events` 
                                            WHERE
                                                person_id IN (
                                                    SELECT
                                                        person_id 
                                                    FROM
                                                        `cb_search_all_events` 
                                                    WHERE
                                                        concept_id IN (
                                                            SELECT
                                                                DISTINCT c.concept_id 
                                                            FROM
                                                                `cb_criteria` c 
                                                            JOIN
                                                                (
                                                                    select
                                                                        cast(cr.id as string) as id 
                                                                    FROM
                                                                        `cb_criteria` cr 
                                                                    WHERE
                                                                        concept_id IN (317309) 
                                                                        AND full_text LIKE '%_rank1]%'
                                                                ) a 
                                                                    ON (
                                                                        c.path LIKE CONCAT('%.',
                                                                    a.id,
                                                                    '.%') 
                                                                    OR c.path LIKE CONCAT('%.',
                                                                    a.id) 
                                                                    OR c.path LIKE CONCAT(a.id,
                                                                    '.%') 
                                                                    OR c.path = a.id) 
                                                                WHERE
                                                                    is_standard = 1 
                                                                    AND is_selectable = 1
                                                                ) 
                                                                AND is_standard = 1 
                                                            UNION
                                                            ALL SELECT
                                                                person_id 
                                                            FROM
                                                                `cb_search_all_events` 
                                                            WHERE
                                                                concept_id IN (
                                                                    SELECT
                                                                        DISTINCT c.concept_id 
                                                                    FROM
                                                                        `cb_criteria` c 
                                                                    JOIN
                                                                        (
                                                                            select
                                                                                cast(cr.id as string) as id 
                                                                            FROM
                                                                                `cb_criteria` cr 
                                                                            WHERE
                                                                                concept_id IN (1569324) 
                                                                                AND full_text LIKE '%_rank1]%'
                                                                        ) a 
                                                                            ON (
                                                                                c.path LIKE CONCAT('%.',
                                                                            a.id,
                                                                            '.%') 
                                                                            OR c.path LIKE CONCAT('%.',
                                                                            a.id) 
                                                                            OR c.path LIKE CONCAT(a.id,
                                                                            '.%') 
                                                                            OR c.path = a.id) 
                                                                        WHERE
                                                                            is_standard = 0 
                                                                            AND is_selectable = 1
                                                                        ) 
                                                                        AND is_standard = 0 
                                                                )
                                                            ) criteria 
                                                    ) 
                                                    AND cb_search_person.person_id IN (
                                                        SELECT
                                                            person_id 
                                                        FROM
                                                            `cb_search_person` p 
                                                        WHERE
                                                            age_at_cdr BETWEEN 40 AND 120 
                                                            AND NOT EXISTS (
                                                                SELECT
                                                                    'x' 
                                                                FROM
                                                                    `death` d 
                                                                WHERE
                                                                    d.person_id = p.person_id
                                                            ) 
                                                        ) 
                                                        AND cb_search_person.person_id IN (
                                                            SELECT
                                                                person_id 
                                                            FROM
                                                                `cb_search_person` p 
                                                            WHERE
                                                                has_ehr_data = 1 
                                                        ) 
                                                    )
                                            )
                                        ) c_occurrence 
                                    LEFT JOIN
                                        `concept` c_standard_concept 
                                            ON c_occurrence.condition_concept_id = c_standard_concept.concept_id 
                                    LEFT JOIN
                                        `concept` c_source_concept 
                                            ON c_occurrence.condition_source_concept_id = c_source_concept.concept_id", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
condition_46344540_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "condition_46344540",
  "condition_46344540_*.csv")
message(str_glue('The data will be written to {condition_46344540_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_46344540_condition_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  condition_46344540_path,
  destination_format = "CSV")



# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {condition_46344540_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- cols(standard_concept_name = col_character(), standard_concept_code = col_character(), condition_source_value = col_character(), source_concept_name = col_character(), source_concept_code = col_character())
  bind_rows(
    map(system2('gsutil', args = c('ls', export_path), stdout = TRUE, stderr = TRUE),
        function(csv) {
          message(str_glue('Loading {csv}.'))
          chunk <- read_csv(pipe(str_glue('gsutil cat {csv}')), col_types = col_types, show_col_types = FALSE)
          if (is.null(col_types)) {
            col_types <- spec(chunk)
          }
          chunk
        }))
}
dataset_46344540_condition_df <- read_bq_export_from_workspace_bucket(condition_46344540_path)

dim(dataset_46344540_condition_df)

head(dataset_46344540_condition_df, 5)

library(tidyverse)
library(bigrquery)

# This query represents dataset "case data" for domain "person" and was generated for All of Us Registered Tier Dataset v7
dataset_46344540_person_sql <- paste("
    SELECT
        person.person_id,
        person.birth_datetime as date_of_birth,
        person.sex_at_birth_concept_id,
        p_sex_at_birth_concept.concept_name as sex_at_birth 
    FROM
        `person` person 
    LEFT JOIN
        `concept` p_sex_at_birth_concept 
            ON person.sex_at_birth_concept_id = p_sex_at_birth_concept.concept_id  
    WHERE
        person.PERSON_ID IN (
            SELECT
                distinct person_id  
            FROM
                `cb_search_person` cb_search_person  
            WHERE
                cb_search_person.person_id IN (
                    SELECT
                        criteria.person_id 
                    FROM
                        (SELECT
                            DISTINCT person_id,
                            entry_date,
                            concept_id 
                        FROM
                            `cb_search_all_events` 
                        WHERE
                            person_id IN (
                                SELECT
                                    person_id 
                                FROM
                                    `cb_search_all_events` 
                                WHERE
                                    concept_id IN (
                                        SELECT
                                            DISTINCT c.concept_id 
                                        FROM
                                            `cb_criteria` c 
                                        JOIN
                                            (
                                                select
                                                    cast(cr.id as string) as id 
                                                FROM
                                                    `cb_criteria` cr 
                                                WHERE
                                                    concept_id IN (317309) 
                                                    AND full_text LIKE '%_rank1]%'
                                            ) a 
                                                ON (
                                                    c.path LIKE CONCAT('%.',
                                                a.id,
                                                '.%') 
                                                OR c.path LIKE CONCAT('%.',
                                                a.id) 
                                                OR c.path LIKE CONCAT(a.id,
                                                '.%') 
                                                OR c.path = a.id) 
                                            WHERE
                                                is_standard = 1 
                                                AND is_selectable = 1
                                            ) 
                                            AND is_standard = 1 
                                        UNION
                                        ALL SELECT
                                            person_id 
                                        FROM
                                            `cb_search_all_events` 
                                        WHERE
                                            concept_id IN (
                                                SELECT
                                                    DISTINCT c.concept_id 
                                                FROM
                                                    `cb_criteria` c 
                                                JOIN
                                                    (
                                                        select
                                                            cast(cr.id as string) as id 
                                                        FROM
                                                            `cb_criteria` cr 
                                                        WHERE
                                                            concept_id IN (1569324) 
                                                            AND full_text LIKE '%_rank1]%'
                                                    ) a 
                                                        ON (
                                                            c.path LIKE CONCAT('%.',
                                                        a.id,
                                                        '.%') 
                                                        OR c.path LIKE CONCAT('%.',
                                                        a.id) 
                                                        OR c.path LIKE CONCAT(a.id,
                                                        '.%') 
                                                        OR c.path = a.id) 
                                                    WHERE
                                                        is_standard = 0 
                                                        AND is_selectable = 1
                                                    ) 
                                                    AND is_standard = 0 
                                            )
                                        ) criteria 
                                ) 
                                AND cb_search_person.person_id IN (
                                    SELECT
                                        person_id 
                                    FROM
                                        `cb_search_person` p 
                                    WHERE
                                        age_at_cdr BETWEEN 40 AND 120 
                                        AND NOT EXISTS (
                                            SELECT
                                                'x' 
                                            FROM
                                                `death` d 
                                            WHERE
                                                d.person_id = p.person_id
                                        ) 
                                    ) 
                                    AND cb_search_person.person_id IN (
                                        SELECT
                                            person_id 
                                        FROM
                                            `cb_search_person` p 
                                        WHERE
                                            has_ehr_data = 1 
                                    ) 
                                )", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
person_46344540_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "person_46344540",
  "person_46344540_*.csv")
message(str_glue('The data will be written to {person_46344540_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_46344540_person_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  person_46344540_path,
  destination_format = "CSV")



# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {person_46344540_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- cols(sex_at_birth = col_character())
  bind_rows(
    map(system2('gsutil', args = c('ls', export_path), stdout = TRUE, stderr = TRUE),
        function(csv) {
          message(str_glue('Loading {csv}.'))
          chunk <- read_csv(pipe(str_glue('gsutil cat {csv}')), col_types = col_types, show_col_types = FALSE)
          if (is.null(col_types)) {
            col_types <- spec(chunk)
          }
          chunk
        }))
}
dataset_46344540_person_df <- read_bq_export_from_workspace_bucket(person_46344540_path)

dim(dataset_46344540_person_df)

head(dataset_46344540_person_df, 5)

library(tidyverse)
library(bigrquery)

# This query represents dataset "case data" for domain "measurement" and was generated for All of Us Registered Tier Dataset v7
dataset_46344540_measurement_sql <- paste("
    SELECT
        measurement.person_id,
        m_standard_concept.concept_name as standard_concept_name,
        measurement.value_as_number 
    FROM
        ( SELECT
            * 
        FROM
            `measurement` measurement 
        WHERE
            (
                measurement_concept_id IN  (
                    SELECT
                        DISTINCT c.concept_id 
                    FROM
                        `cb_criteria` c 
                    JOIN
                        (
                            select
                                cast(cr.id as string) as id 
                            FROM
                                `cb_criteria` cr 
                            WHERE
                                concept_id IN (
                                    3004249, 3012888, 3027114, 37065054, 40789150, 40789166
                                ) 
                                AND full_text LIKE '%_rank1]%'
                        ) a 
                            ON (
                                c.path LIKE CONCAT('%.',
                            a.id,
                            '.%') 
                            OR c.path LIKE CONCAT('%.',
                            a.id) 
                            OR c.path LIKE CONCAT(a.id,
                            '.%') 
                            OR c.path = a.id) 
                        WHERE
                            is_standard = 1 
                            AND is_selectable = 1
                        ) 
                        OR  measurement_source_concept_id IN  (
                            SELECT
                                DISTINCT c.concept_id 
                            FROM
                                `cb_criteria` c 
                            JOIN
                                (
                                    select
                                        cast(cr.id as string) as id 
                                    FROM
                                        `cb_criteria` cr 
                                    WHERE
                                        concept_id IN (
                                            903124
                                        ) 
                                        AND full_text LIKE '%_rank1]%'
                                ) a 
                                    ON (
                                        c.path LIKE CONCAT('%.',
                                    a.id,
                                    '.%') 
                                    OR c.path LIKE CONCAT('%.',
                                    a.id) 
                                    OR c.path LIKE CONCAT(a.id,
                                    '.%') 
                                    OR c.path = a.id) 
                                WHERE
                                    is_standard = 0 
                                    AND is_selectable = 1
                                )
                        )  
                        AND (
                            measurement.PERSON_ID IN (
                                SELECT
                                    distinct person_id  
                                FROM
                                    `cb_search_person` cb_search_person  
                                WHERE
                                    cb_search_person.person_id IN (
                                        SELECT
                                            criteria.person_id 
                                        FROM
                                            (SELECT
                                                DISTINCT person_id,
                                                entry_date,
                                                concept_id 
                                            FROM
                                                `cb_search_all_events` 
                                            WHERE
                                                person_id IN (
                                                    SELECT
                                                        person_id 
                                                    FROM
                                                        `cb_search_all_events` 
                                                    WHERE
                                                        concept_id IN (
                                                            SELECT
                                                                DISTINCT c.concept_id 
                                                            FROM
                                                                `cb_criteria` c 
                                                            JOIN
                                                                (
                                                                    select
                                                                        cast(cr.id as string) as id 
                                                                    FROM
                                                                        `cb_criteria` cr 
                                                                    WHERE
                                                                        concept_id IN (317309) 
                                                                        AND full_text LIKE '%_rank1]%'
                                                                ) a 
                                                                    ON (
                                                                        c.path LIKE CONCAT('%.',
                                                                    a.id,
                                                                    '.%') 
                                                                    OR c.path LIKE CONCAT('%.',
                                                                    a.id) 
                                                                    OR c.path LIKE CONCAT(a.id,
                                                                    '.%') 
                                                                    OR c.path = a.id) 
                                                                WHERE
                                                                    is_standard = 1 
                                                                    AND is_selectable = 1
                                                                ) 
                                                                AND is_standard = 1 
                                                            UNION
                                                            ALL SELECT
                                                                person_id 
                                                            FROM
                                                                `cb_search_all_events` 
                                                            WHERE
                                                                concept_id IN (
                                                                    SELECT
                                                                        DISTINCT c.concept_id 
                                                                    FROM
                                                                        `cb_criteria` c 
                                                                    JOIN
                                                                        (
                                                                            select
                                                                                cast(cr.id as string) as id 
                                                                            FROM
                                                                                `cb_criteria` cr 
                                                                            WHERE
                                                                                concept_id IN (1569324) 
                                                                                AND full_text LIKE '%_rank1]%'
                                                                        ) a 
                                                                            ON (
                                                                                c.path LIKE CONCAT('%.',
                                                                            a.id,
                                                                            '.%') 
                                                                            OR c.path LIKE CONCAT('%.',
                                                                            a.id) 
                                                                            OR c.path LIKE CONCAT(a.id,
                                                                            '.%') 
                                                                            OR c.path = a.id) 
                                                                        WHERE
                                                                            is_standard = 0 
                                                                            AND is_selectable = 1
                                                                        ) 
                                                                        AND is_standard = 0 
                                                                )
                                                            ) criteria 
                                                    ) 
                                                    AND cb_search_person.person_id IN (
                                                        SELECT
                                                            person_id 
                                                        FROM
                                                            `cb_search_person` p 
                                                        WHERE
                                                            age_at_cdr BETWEEN 40 AND 120 
                                                            AND NOT EXISTS (
                                                                SELECT
                                                                    'x' 
                                                                FROM
                                                                    `death` d 
                                                                WHERE
                                                                    d.person_id = p.person_id
                                                            ) 
                                                        ) 
                                                        AND cb_search_person.person_id IN (
                                                            SELECT
                                                                person_id 
                                                            FROM
                                                                `cb_search_person` p 
                                                            WHERE
                                                                has_ehr_data = 1 
                                                        ) 
                                                    )
                                            )
                                        ) measurement 
                                    LEFT JOIN
                                        `concept` m_standard_concept 
                                            ON measurement.measurement_concept_id = m_standard_concept.concept_id", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
measurement_46344540_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "measurement_46344540",
  "measurement_46344540_*.csv")
message(str_glue('The data will be written to {measurement_46344540_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_46344540_measurement_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  measurement_46344540_path,
  destination_format = "CSV")



# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {measurement_46344540_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- cols(standard_concept_name = col_character())
  bind_rows(
    map(system2('gsutil', args = c('ls', export_path), stdout = TRUE, stderr = TRUE),
        function(csv) {
          message(str_glue('Loading {csv}.'))
          chunk <- read_csv(pipe(str_glue('gsutil cat {csv}')), col_types = col_types, show_col_types = FALSE)
          if (is.null(col_types)) {
            col_types <- spec(chunk)
          }
          chunk
        }))
}
dataset_46344540_measurement_df <- read_bq_export_from_workspace_bucket(measurement_46344540_path)

dim(dataset_46344540_measurement_df)

head(dataset_46344540_measurement_df, 5)

library(tidyverse)
library(bigrquery)

# This query represents dataset "control data" for domain "survey" and was generated for All of Us Registered Tier Dataset v7
dataset_54753455_survey_sql <- paste("
    SELECT
        answer.person_id,
        answer.survey_datetime,
        answer.question,
        answer.answer  
    FROM
        `ds_survey` answer   
    WHERE
        (
            question_concept_id IN (
                1585741, 1585873, 1586198
            )
        )  
        AND (
            answer.PERSON_ID IN (
                SELECT
                    distinct person_id  
                FROM
                    `cb_search_person` cb_search_person  
                WHERE
                    cb_search_person.person_id IN (
                        SELECT
                            person_id 
                        FROM
                            `cb_search_person` p 
                        WHERE
                            has_ehr_data = 1 
                    ) 
                    AND cb_search_person.person_id IN (
                        SELECT
                            person_id 
                        FROM
                            `cb_search_person` p 
                        WHERE
                            age_at_cdr BETWEEN 40 AND 120 
                            AND NOT EXISTS (
                                SELECT
                                    'x' 
                                FROM
                                    `death` d 
                                WHERE
                                    d.person_id = p.person_id
                            ) 
                        ) 
                        AND cb_search_person.person_id NOT IN (
                            SELECT
                                criteria.person_id 
                            FROM
                                (SELECT
                                    DISTINCT person_id,
                                    entry_date,
                                    concept_id 
                                FROM
                                    `cb_search_all_events` 
                                WHERE
                                    person_id IN (
                                        SELECT
                                            person_id 
                                        FROM
                                            `cb_search_all_events` 
                                        WHERE
                                            concept_id IN (
                                                SELECT
                                                    DISTINCT c.concept_id 
                                                FROM
                                                    `cb_criteria` c 
                                                JOIN
                                                    (
                                                        select
                                                            cast(cr.id as string) as id 
                                                        FROM
                                                            `cb_criteria` cr 
                                                        WHERE
                                                            concept_id IN (317309) 
                                                            AND full_text LIKE '%_rank1]%'
                                                    ) a 
                                                        ON (
                                                            c.path LIKE CONCAT('%.',
                                                        a.id,
                                                        '.%') 
                                                        OR c.path LIKE CONCAT('%.',
                                                        a.id) 
                                                        OR c.path LIKE CONCAT(a.id,
                                                        '.%') 
                                                        OR c.path = a.id) 
                                                    WHERE
                                                        is_standard = 1 
                                                        AND is_selectable = 1
                                                    ) 
                                                    AND is_standard = 1 
                                                UNION
                                                ALL SELECT
                                                    person_id 
                                                FROM
                                                    `cb_search_all_events` 
                                                WHERE
                                                    concept_id IN (
                                                        SELECT
                                                            DISTINCT c.concept_id 
                                                        FROM
                                                            `cb_criteria` c 
                                                        JOIN
                                                            (
                                                                select
                                                                    cast(cr.id as string) as id 
                                                                FROM
                                                                    `cb_criteria` cr 
                                                                WHERE
                                                                    concept_id IN (1569324, 1569327, 1569323, 1569329, 45567227, 1569334) 
                                                                    AND full_text LIKE '%_rank1]%'
                                                            ) a 
                                                                ON (
                                                                    c.path LIKE CONCAT('%.',
                                                                a.id,
                                                                '.%') 
                                                                OR c.path LIKE CONCAT('%.',
                                                                a.id) 
                                                                OR c.path LIKE CONCAT(a.id,
                                                                '.%') 
                                                                OR c.path = a.id) 
                                                            WHERE
                                                                is_standard = 0 
                                                                AND is_selectable = 1
                                                            ) 
                                                            AND is_standard = 0 
                                                    )
                                                ) criteria 
                                        ) 
                                )
                            )", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
survey_54753455_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "survey_54753455",
  "survey_54753455_*.csv")
message(str_glue('The data will be written to {survey_54753455_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_54753455_survey_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  survey_54753455_path,
  destination_format = "CSV")



# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {survey_54753455_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- cols(survey = col_character(), question = col_character(), answer = col_character())
  bind_rows(
    map(system2('gsutil', args = c('ls', export_path), stdout = TRUE, stderr = TRUE),
        function(csv) {
          message(str_glue('Loading {csv}.'))
          chunk <- read_csv(pipe(str_glue('gsutil cat {csv}')), col_types = col_types, show_col_types = FALSE)
          if (is.null(col_types)) {
            col_types <- spec(chunk)
          }
          chunk
        }))
}
dataset_54753455_survey_df <- read_bq_export_from_workspace_bucket(survey_54753455_path)

dim(dataset_54753455_survey_df)

head(dataset_54753455_survey_df, 5)

library(tidyverse)
library(bigrquery)

# This query represents dataset "control data" for domain "condition" and was generated for All of Us Registered Tier Dataset v7
dataset_54753455_condition_sql <- paste("
    SELECT
        c_occurrence.person_id,
        c_occurrence.condition_concept_id,
        c_standard_concept.concept_name as standard_concept_name,
        c_standard_concept.concept_code as standard_concept_code,
        c_occurrence.condition_start_datetime,
        c_source_concept.concept_name as source_concept_name,
        c_source_concept.concept_code as source_concept_code 
    FROM
        ( SELECT
            * 
        FROM
            `condition_occurrence` c_occurrence 
        WHERE
            (
                condition_concept_id IN  (
                    SELECT
                        DISTINCT c.concept_id 
                    FROM
                        `cb_criteria` c 
                    JOIN
                        (
                            select
                                cast(cr.id as string) as id 
                            FROM
                                `cb_criteria` cr 
                            WHERE
                                concept_id IN (
                                    201820, 316866, 4024561, 432867
                                ) 
                                AND full_text LIKE '%_rank1]%'
                        ) a 
                            ON (
                                c.path LIKE CONCAT('%.',
                            a.id,
                            '.%') 
                            OR c.path LIKE CONCAT('%.',
                            a.id) 
                            OR c.path LIKE CONCAT(a.id,
                            '.%') 
                            OR c.path = a.id) 
                        WHERE
                            is_standard = 1 
                            AND is_selectable = 1
                        ) 
                        OR  condition_source_concept_id IN  (
                            SELECT
                                DISTINCT c.concept_id 
                            FROM
                                `cb_criteria` c 
                            JOIN
                                (
                                    select
                                        cast(cr.id as string) as id 
                                    FROM
                                        `cb_criteria` cr 
                                    WHERE
                                        concept_id IN (
                                            35207668, 44819500, 44819501, 44819502, 44819503, 44819504, 44820682, 44820683, 44820684, 44820685, 44821787, 44822934, 44822935, 44822936, 44824071, 44824072, 44824073, 44824074, 44825264, 44826459, 44826460, 44826461, 44827615, 44827616, 44827617, 44828793, 44828794, 44828795, 44829878, 44829879, 44829880, 44829881, 44829882, 44831045, 44831046, 44831047, 44832190, 44832191, 44832192, 44832193, 44832194, 44833365, 44833366, 44833367, 44833368, 44834548, 44834549, 44836914, 44836915, 44836916, 44836917, 44836918
                                        ) 
                                        AND full_text LIKE '%_rank1]%'
                                ) a 
                                    ON (
                                        c.path LIKE CONCAT('%.',
                                    a.id,
                                    '.%') 
                                    OR c.path LIKE CONCAT('%.',
                                    a.id) 
                                    OR c.path LIKE CONCAT(a.id,
                                    '.%') 
                                    OR c.path = a.id) 
                                WHERE
                                    is_standard = 0 
                                    AND is_selectable = 1
                                )
                        )  
                        AND (
                            c_occurrence.PERSON_ID IN (
                                SELECT
                                    distinct person_id  
                                FROM
                                    `cb_search_person` cb_search_person  
                                WHERE
                                    cb_search_person.person_id IN (
                                        SELECT
                                            person_id 
                                        FROM
                                            `cb_search_person` p 
                                        WHERE
                                            has_ehr_data = 1 
                                    ) 
                                    AND cb_search_person.person_id IN (
                                        SELECT
                                            person_id 
                                        FROM
                                            `cb_search_person` p 
                                        WHERE
                                            age_at_cdr BETWEEN 40 AND 120 
                                            AND NOT EXISTS (
                                                SELECT
                                                    'x' 
                                                FROM
                                                    `death` d 
                                                WHERE
                                                    d.person_id = p.person_id
                                            ) 
                                        ) 
                                        AND cb_search_person.person_id NOT IN (
                                            SELECT
                                                criteria.person_id 
                                            FROM
                                                (SELECT
                                                    DISTINCT person_id,
                                                    entry_date,
                                                    concept_id 
                                                FROM
                                                    `cb_search_all_events` 
                                                WHERE
                                                    person_id IN (
                                                        SELECT
                                                            person_id 
                                                        FROM
                                                            `cb_search_all_events` 
                                                        WHERE
                                                            concept_id IN (
                                                                SELECT
                                                                    DISTINCT c.concept_id 
                                                                FROM
                                                                    `cb_criteria` c 
                                                                JOIN
                                                                    (
                                                                        select
                                                                            cast(cr.id as string) as id 
                                                                        FROM
                                                                            `cb_criteria` cr 
                                                                        WHERE
                                                                            concept_id IN (317309) 
                                                                            AND full_text LIKE '%_rank1]%'
                                                                    ) a 
                                                                        ON (
                                                                            c.path LIKE CONCAT('%.',
                                                                        a.id,
                                                                        '.%') 
                                                                        OR c.path LIKE CONCAT('%.',
                                                                        a.id) 
                                                                        OR c.path LIKE CONCAT(a.id,
                                                                        '.%') 
                                                                        OR c.path = a.id) 
                                                                    WHERE
                                                                        is_standard = 1 
                                                                        AND is_selectable = 1
                                                                    ) 
                                                                    AND is_standard = 1 
                                                                UNION
                                                                ALL SELECT
                                                                    person_id 
                                                                FROM
                                                                    `cb_search_all_events` 
                                                                WHERE
                                                                    concept_id IN (
                                                                        SELECT
                                                                            DISTINCT c.concept_id 
                                                                        FROM
                                                                            `cb_criteria` c 
                                                                        JOIN
                                                                            (
                                                                                select
                                                                                    cast(cr.id as string) as id 
                                                                                FROM
                                                                                    `cb_criteria` cr 
                                                                                WHERE
                                                                                    concept_id IN (1569324, 1569327, 1569323, 1569329, 45567227, 1569334) 
                                                                                    AND full_text LIKE '%_rank1]%'
                                                                            ) a 
                                                                                ON (
                                                                                    c.path LIKE CONCAT('%.',
                                                                                a.id,
                                                                                '.%') 
                                                                                OR c.path LIKE CONCAT('%.',
                                                                                a.id) 
                                                                                OR c.path LIKE CONCAT(a.id,
                                                                                '.%') 
                                                                                OR c.path = a.id) 
                                                                            WHERE
                                                                                is_standard = 0 
                                                                                AND is_selectable = 1
                                                                            ) 
                                                                            AND is_standard = 0 
                                                                    )
                                                                ) criteria 
                                                        ) 
                                                )
                                            )
                                    ) c_occurrence 
                                LEFT JOIN
                                    `concept` c_standard_concept 
                                        ON c_occurrence.condition_concept_id = c_standard_concept.concept_id 
                                LEFT JOIN
                                    `concept` c_source_concept 
                                        ON c_occurrence.condition_source_concept_id = c_source_concept.concept_id", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
condition_54753455_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "condition_54753455",
  "condition_54753455_*.csv")
message(str_glue('The data will be written to {condition_54753455_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_54753455_condition_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  condition_54753455_path,
  destination_format = "CSV")



# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {condition_54753455_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- cols(standard_concept_name = col_character(), standard_concept_code = col_character(), source_concept_name = col_character(), source_concept_code = col_character())
  bind_rows(
    map(system2('gsutil', args = c('ls', export_path), stdout = TRUE, stderr = TRUE),
        function(csv) {
          message(str_glue('Loading {csv}.'))
          chunk <- read_csv(pipe(str_glue('gsutil cat {csv}')), col_types = col_types, show_col_types = FALSE)
          if (is.null(col_types)) {
            col_types <- spec(chunk)
          }
          chunk
        }))
}
dataset_54753455_condition_df <- read_bq_export_from_workspace_bucket(condition_54753455_path)

dim(dataset_54753455_condition_df)

head(dataset_54753455_condition_df, 5)

library(tidyverse)
library(bigrquery)

# This query represents dataset "control data" for domain "person" and was generated for All of Us Registered Tier Dataset v7
dataset_54753455_person_sql <- paste("
    SELECT
        person.person_id,
        person.birth_datetime as date_of_birth,
        person.sex_at_birth_concept_id,
        p_sex_at_birth_concept.concept_name as sex_at_birth 
    FROM
        `person` person 
    LEFT JOIN
        `concept` p_sex_at_birth_concept 
            ON person.sex_at_birth_concept_id = p_sex_at_birth_concept.concept_id  
    WHERE
        person.PERSON_ID IN (
            SELECT
                distinct person_id  
            FROM
                `cb_search_person` cb_search_person  
            WHERE
                cb_search_person.person_id IN (
                    SELECT
                        person_id 
                    FROM
                        `cb_search_person` p 
                    WHERE
                        has_ehr_data = 1 
                ) 
                AND cb_search_person.person_id IN (
                    SELECT
                        person_id 
                    FROM
                        `cb_search_person` p 
                    WHERE
                        age_at_cdr BETWEEN 40 AND 120 
                        AND NOT EXISTS (
                            SELECT
                                'x' 
                            FROM
                                `death` d 
                            WHERE
                                d.person_id = p.person_id
                        ) 
                    ) 
                    AND cb_search_person.person_id NOT IN (
                        SELECT
                            criteria.person_id 
                        FROM
                            (SELECT
                                DISTINCT person_id,
                                entry_date,
                                concept_id 
                            FROM
                                `cb_search_all_events` 
                            WHERE
                                person_id IN (
                                    SELECT
                                        person_id 
                                    FROM
                                        `cb_search_all_events` 
                                    WHERE
                                        concept_id IN (
                                            SELECT
                                                DISTINCT c.concept_id 
                                            FROM
                                                `cb_criteria` c 
                                            JOIN
                                                (
                                                    select
                                                        cast(cr.id as string) as id 
                                                    FROM
                                                        `cb_criteria` cr 
                                                    WHERE
                                                        concept_id IN (317309) 
                                                        AND full_text LIKE '%_rank1]%'
                                                ) a 
                                                    ON (
                                                        c.path LIKE CONCAT('%.',
                                                    a.id,
                                                    '.%') 
                                                    OR c.path LIKE CONCAT('%.',
                                                    a.id) 
                                                    OR c.path LIKE CONCAT(a.id,
                                                    '.%') 
                                                    OR c.path = a.id) 
                                                WHERE
                                                    is_standard = 1 
                                                    AND is_selectable = 1
                                                ) 
                                                AND is_standard = 1 
                                            UNION
                                            ALL SELECT
                                                person_id 
                                            FROM
                                                `cb_search_all_events` 
                                            WHERE
                                                concept_id IN (
                                                    SELECT
                                                        DISTINCT c.concept_id 
                                                    FROM
                                                        `cb_criteria` c 
                                                    JOIN
                                                        (
                                                            select
                                                                cast(cr.id as string) as id 
                                                            FROM
                                                                `cb_criteria` cr 
                                                            WHERE
                                                                concept_id IN (1569324, 1569327, 1569323, 1569329, 45567227, 1569334) 
                                                                AND full_text LIKE '%_rank1]%'
                                                        ) a 
                                                            ON (
                                                                c.path LIKE CONCAT('%.',
                                                            a.id,
                                                            '.%') 
                                                            OR c.path LIKE CONCAT('%.',
                                                            a.id) 
                                                            OR c.path LIKE CONCAT(a.id,
                                                            '.%') 
                                                            OR c.path = a.id) 
                                                        WHERE
                                                            is_standard = 0 
                                                            AND is_selectable = 1
                                                        ) 
                                                        AND is_standard = 0 
                                                )
                                            ) criteria 
                                    ) 
                            )", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
person_54753455_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "person_54753455",
  "person_54753455_*.csv")
message(str_glue('The data will be written to {person_54753455_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_54753455_person_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  person_54753455_path,
  destination_format = "CSV")



# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {person_54753455_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- cols(sex_at_birth = col_character())
  bind_rows(
    map(system2('gsutil', args = c('ls', export_path), stdout = TRUE, stderr = TRUE),
        function(csv) {
          message(str_glue('Loading {csv}.'))
          chunk <- read_csv(pipe(str_glue('gsutil cat {csv}')), col_types = col_types, show_col_types = FALSE)
          if (is.null(col_types)) {
            col_types <- spec(chunk)
          }
          chunk
        }))
}
dataset_54753455_person_df <- read_bq_export_from_workspace_bucket(person_54753455_path)

dim(dataset_54753455_person_df)

head(dataset_54753455_person_df, 5)

library(tidyverse)
library(bigrquery)

# This query represents dataset "control data" for domain "measurement" and was generated for All of Us Registered Tier Dataset v7
dataset_54753455_measurement_sql <- paste("
    SELECT
        measurement.person_id,
        m_standard_concept.concept_name as standard_concept_name,
        measurement.value_as_number 
    FROM
        ( SELECT
            * 
        FROM
            `measurement` measurement 
        WHERE
            (
                measurement_concept_id IN  (
                    SELECT
                        DISTINCT c.concept_id 
                    FROM
                        `cb_criteria` c 
                    JOIN
                        (
                            select
                                cast(cr.id as string) as id 
                            FROM
                                `cb_criteria` cr 
                            WHERE
                                concept_id IN (
                                    3004249, 3012888, 3027114, 37065054, 40789150, 40789166
                                ) 
                                AND full_text LIKE '%_rank1]%'
                        ) a 
                            ON (
                                c.path LIKE CONCAT('%.',
                            a.id,
                            '.%') 
                            OR c.path LIKE CONCAT('%.',
                            a.id) 
                            OR c.path LIKE CONCAT(a.id,
                            '.%') 
                            OR c.path = a.id) 
                        WHERE
                            is_standard = 1 
                            AND is_selectable = 1
                        ) 
                        OR  measurement_source_concept_id IN  (
                            SELECT
                                DISTINCT c.concept_id 
                            FROM
                                `cb_criteria` c 
                            JOIN
                                (
                                    select
                                        cast(cr.id as string) as id 
                                    FROM
                                        `cb_criteria` cr 
                                    WHERE
                                        concept_id IN (
                                            903124
                                        ) 
                                        AND full_text LIKE '%_rank1]%'
                                ) a 
                                    ON (
                                        c.path LIKE CONCAT('%.',
                                    a.id,
                                    '.%') 
                                    OR c.path LIKE CONCAT('%.',
                                    a.id) 
                                    OR c.path LIKE CONCAT(a.id,
                                    '.%') 
                                    OR c.path = a.id) 
                                WHERE
                                    is_standard = 0 
                                    AND is_selectable = 1
                                )
                        )  
                        AND (
                            measurement.PERSON_ID IN (
                                SELECT
                                    distinct person_id  
                                FROM
                                    `cb_search_person` cb_search_person  
                                WHERE
                                    cb_search_person.person_id IN (
                                        SELECT
                                            person_id 
                                        FROM
                                            `cb_search_person` p 
                                        WHERE
                                            has_ehr_data = 1 
                                    ) 
                                    AND cb_search_person.person_id IN (
                                        SELECT
                                            person_id 
                                        FROM
                                            `cb_search_person` p 
                                        WHERE
                                            age_at_cdr BETWEEN 40 AND 120 
                                            AND NOT EXISTS (
                                                SELECT
                                                    'x' 
                                                FROM
                                                    `death` d 
                                                WHERE
                                                    d.person_id = p.person_id
                                            ) 
                                        ) 
                                        AND cb_search_person.person_id NOT IN (
                                            SELECT
                                                criteria.person_id 
                                            FROM
                                                (SELECT
                                                    DISTINCT person_id,
                                                    entry_date,
                                                    concept_id 
                                                FROM
                                                    `cb_search_all_events` 
                                                WHERE
                                                    person_id IN (
                                                        SELECT
                                                            person_id 
                                                        FROM
                                                            `cb_search_all_events` 
                                                        WHERE
                                                            concept_id IN (
                                                                SELECT
                                                                    DISTINCT c.concept_id 
                                                                FROM
                                                                    `cb_criteria` c 
                                                                JOIN
                                                                    (
                                                                        select
                                                                            cast(cr.id as string) as id 
                                                                        FROM
                                                                            `cb_criteria` cr 
                                                                        WHERE
                                                                            concept_id IN (317309) 
                                                                            AND full_text LIKE '%_rank1]%'
                                                                    ) a 
                                                                        ON (
                                                                            c.path LIKE CONCAT('%.',
                                                                        a.id,
                                                                        '.%') 
                                                                        OR c.path LIKE CONCAT('%.',
                                                                        a.id) 
                                                                        OR c.path LIKE CONCAT(a.id,
                                                                        '.%') 
                                                                        OR c.path = a.id) 
                                                                    WHERE
                                                                        is_standard = 1 
                                                                        AND is_selectable = 1
                                                                    ) 
                                                                    AND is_standard = 1 
                                                                UNION
                                                                ALL SELECT
                                                                    person_id 
                                                                FROM
                                                                    `cb_search_all_events` 
                                                                WHERE
                                                                    concept_id IN (
                                                                        SELECT
                                                                            DISTINCT c.concept_id 
                                                                        FROM
                                                                            `cb_criteria` c 
                                                                        JOIN
                                                                            (
                                                                                select
                                                                                    cast(cr.id as string) as id 
                                                                                FROM
                                                                                    `cb_criteria` cr 
                                                                                WHERE
                                                                                    concept_id IN (1569324, 1569327, 1569323, 1569329, 45567227, 1569334) 
                                                                                    AND full_text LIKE '%_rank1]%'
                                                                            ) a 
                                                                                ON (
                                                                                    c.path LIKE CONCAT('%.',
                                                                                a.id,
                                                                                '.%') 
                                                                                OR c.path LIKE CONCAT('%.',
                                                                                a.id) 
                                                                                OR c.path LIKE CONCAT(a.id,
                                                                                '.%') 
                                                                                OR c.path = a.id) 
                                                                            WHERE
                                                                                is_standard = 0 
                                                                                AND is_selectable = 1
                                                                            ) 
                                                                            AND is_standard = 0 
                                                                    )
                                                                ) criteria 
                                                        ) 
                                                )
                                            )
                                    ) measurement 
                                LEFT JOIN
                                    `concept` m_standard_concept 
                                        ON measurement.measurement_concept_id = m_standard_concept.concept_id", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
measurement_54753455_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "measurement_54753455",
  "measurement_54753455_*.csv")
message(str_glue('The data will be written to {measurement_54753455_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_54753455_measurement_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  measurement_54753455_path,
  destination_format = "CSV")



# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {measurement_54753455_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- cols(standard_concept_name = col_character())
  bind_rows(
    map(system2('gsutil', args = c('ls', export_path), stdout = TRUE, stderr = TRUE),
        function(csv) {
          message(str_glue('Loading {csv}.'))
          chunk <- read_csv(pipe(str_glue('gsutil cat {csv}')), col_types = col_types, show_col_types = FALSE)
          if (is.null(col_types)) {
            col_types <- spec(chunk)
          }
          chunk
        }))
}
dataset_54753455_measurement_df <- read_bq_export_from_workspace_bucket(measurement_54753455_path)

dim(dataset_54753455_measurement_df)

head(dataset_54753455_measurement_df, 5)

case_survey = dataset_46344540_survey_df
case_condition = dataset_46344540_condition_df
case_measure = dataset_46344540_measurement_df
case_person = dataset_46344540_person_df

table(case_measure$standard_concept_name)

case_BP1 = case_measure %>%
    filter(grepl("Systolic", standard_concept_name)) %>%
    mutate(sys = value_as_number) %>%
    arrange(desc(sys)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, sys)

case_BP2 = case_measure %>%
    filter(grepl("Diastolic", standard_concept_name)) %>%
    mutate(dia = value_as_number) %>%
    arrange(desc(dia)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, dia)

case_BP = case_BP1 %>%
    left_join(case_BP2, by = "person_id") %>%
    mutate(BP = sys/3 + 2*dia/3) %>%
    select(person_id, BP)

case_BG = case_measure %>%
    filter(grepl("Glucose", standard_concept_name)) %>%
    mutate(BG = value_as_number) %>%
    arrange(desc(BG)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, BG)

case_BC = case_measure %>%
    filter(grepl("Cholesterol", standard_concept_name)) %>%
    mutate(BC = value_as_number) %>%
    arrange(desc(BC)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, BC)

case_hom = case_measure %>%
    filter(standard_concept_name == "Homocysteine [Moles/volume] in Serum or Plasma") %>%
    mutate(hom = value_as_number) %>%
    arrange(desc(hom)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, hom)

case_fib = case_measure %>%
    filter(standard_concept_name == "Fibrinogen [Mass/volume] in Platelet poor plasma by Coagulation assay") %>%
    mutate(fib = value_as_number) %>%
    arrange(desc(fib)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, fib)

case_pain = case_condition %>%
    mutate(pain = grepl("pain", standard_concept_name)) %>%
    arrange(desc(pain)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, pain)

case_smk = case_survey %>%
    filter(grepl("Smoking", question)) %>%
    mutate(smk = answer > 0) %>%
    arrange(desc(smk)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, smk)

case_alc = case_survey %>%
    filter(grepl("Alcohol", question)) %>%
    mutate(alc = answer > 0) %>%
    arrange(desc(alc)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, alc)

case_exr = case_survey %>%
    filter(grepl("Overall", question)) %>%
    mutate(exr = answer > 0) %>%
    arrange(desc(exr)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, exr)

case_bmi = case_measure %>%
    filter(grepl("Body", standard_concept_name)) %>%
    mutate(bmi = value_as_number) %>%
    arrange(desc(bmi)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, bmi)

case_demo = case_person %>%
    mutate(date_of_birth = as.Date(date_of_birth)) %>%
    mutate(age = as.numeric(as.Date("2023-01-01") - date_of_birth)/365) %>%
    mutate(sex = (sex_at_birth == "Male")) %>%
    select(person_id, age, sex)

case_var = left_join(case_pain, case_smk, by = "person_id") %>%
    left_join(case_alc, by = "person_id") %>%
    left_join(case_exr, by = "person_id") %>%
    left_join(case_bmi, by = "person_id") %>%
    left_join(case_demo, by = "person_id") %>%
    left_join(case_BC, by = "person_id") %>%
    left_join(case_BG, by = "person_id") %>%
    left_join(case_BP, by = "person_id") %>%
    left_join(case_hom, by = "person_id") %>%
    left_join(case_fib, by = "person_id") %>%
    mutate(PAD = 1) %>%
    mutate_all(~ifelse(is.na(.), FALSE, .))
head(case_var)

control_survey = dataset_54753455_survey_df
control_condition = dataset_54753455_condition_df
control_measure = dataset_54753455_measurement_df
control_person = dataset_54753455_person_df

table(control_measure$standard_concept_name)

control_BP1 = control_measure %>%
    filter(grepl("Systolic", standard_concept_name)) %>%
    mutate(sys = value_as_number) %>%
    arrange(desc(sys)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, sys)

control_BP2 = control_measure %>%
    filter(grepl("Diastolic", standard_concept_name)) %>%
    mutate(dia = value_as_number) %>%
    arrange(desc(dia)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, dia)

control_BP = control_BP1 %>%
    left_join(control_BP2, by = "person_id") %>%
    mutate(BP = sys/3 + 2*dia/3) %>%
    select(person_id, BP)

control_BG = control_measure %>%
    filter(grepl("Glucose", standard_concept_name)) %>%
    mutate(BG = value_as_number) %>%
    arrange(desc(BG)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, BG)

control_BC = control_measure %>%
    filter(grepl("Cholesterol", standard_concept_name)) %>%
    mutate(BC = value_as_number) %>%
    arrange(desc(BC)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, BC)

control_hom = control_measure %>%
    filter(standard_concept_name == "Homocysteine [Moles/volume] in Serum or Plasma") %>%
    mutate(hom = value_as_number) %>%
    arrange(desc(hom)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, hom)

control_fib = control_measure %>%
    filter(standard_concept_name == "Fibrinogen [Mass/volume] in Platelet poor plasma by Coagulation assay") %>%
    mutate(fib = value_as_number) %>%
    arrange(desc(fib)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, fib)

control_pain = control_condition %>%
    mutate(pain = grepl("pain", standard_concept_name)) %>%
    arrange(desc(pain)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, pain)

control_smk = control_survey %>%
    filter(grepl("Smoking", question)) %>%
    mutate(smk = answer > 0) %>%
    arrange(desc(smk)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, smk)

control_alc = control_survey %>%
    filter(grepl("Alcohol", question)) %>%
    mutate(alc = answer > 0) %>%
    arrange(desc(alc)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, alc)

control_exr = control_survey %>%
    filter(grepl("Overall", question)) %>%
    mutate(exr = answer > 0) %>%
    arrange(desc(exr)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, exr)

control_bmi = control_measure %>%
    filter(grepl("Body", standard_concept_name)) %>%
    mutate(bmi = value_as_number) %>%
    arrange(desc(bmi)) %>%
    distinct(person_id, .keep_all = TRUE) %>%
    select(person_id, bmi)

control_demo = control_person %>%
    mutate(date_of_birth = as.Date(date_of_birth)) %>%
    mutate(age = as.numeric(as.Date("2023-01-01") - date_of_birth)/365) %>%
    mutate(sex = (sex_at_birth == "Male")) %>%
    select(person_id, age, sex)

control_var = left_join(control_pain, control_smk, by = "person_id") %>%
    left_join(control_alc, by = "person_id") %>%
    left_join(control_exr, by = "person_id") %>%
    left_join(control_bmi, by = "person_id") %>%
    left_join(control_demo, by = "person_id") %>%
    left_join(control_BC, by = "person_id") %>%
    left_join(control_BG, by = "person_id") %>%
    left_join(control_BP, by = "person_id") %>%
    left_join(control_hom, by = "person_id") %>%
    left_join(control_fib, by = "person_id") %>%
    mutate(PAD = 0) %>%
    mutate_all(~ifelse(is.na(.), FALSE, .))
head(control_var)
# table(control_var$fib)

all_var = rbind(case_var, control_var) %>% 
    mutate_all(as.numeric) %>%
    mutate(PAD = as.integer(PAD))
head(all_var)
# length(unique(filter(all_var, hom != 0 & PAD == 1)$person_id))

# write.csv(all_var, file = "all_var.csv", row.names = FALSE)

# read_csv("/home/jupyter/workspaces/padprediction/all_var.csv")

# This snippet assumes that you run setup first

# This code saves your dataframe into a csv file in a "data" folder in Google Bucket

# Replace df with THE NAME OF YOUR DATAFRAME
my_dataframe <- all_var

# Replace 'test.csv' with THE NAME of the file you're going to store in the bucket (don't delete the quotation marks)
destination_filename <- 'all_var.csv'

########################################################################
##
################# DON'T CHANGE FROM HERE ###############################
##
########################################################################

# store the dataframe in current workspace
write_excel_csv(my_dataframe, destination_filename)

# Get the bucket name
my_bucket <- Sys.getenv('WORKSPACE_BUCKET')

# Copy the file from current workspace to the bucket
system(paste0("gsutil cp ./", destination_filename, " ", my_bucket, "/data/"), intern=T)

# Check if file is in the bucket
system(paste0("gsutil ls ", my_bucket, "/data/*.csv"), intern=T)


# Assuming case_var is your dataframe
case_var <- subset(case_var, BC != 0 & BC < 1000)
case_var <- subset(case_var, BG != 0 & BG < 1000)
case_var <- subset(case_var, BP != 0 & BP < 1000)
case_var <- subset(case_var, bmi != 0 & bmi < 1000)
case_var <- subset(case_var, age != 0 & age < 1000)
case_var <- subset(case_var, fib != 0 & fib < 10000)
# Assuming control_var is your dataframe
control_var <- subset(control_var, BC != 0 & BC < 1000)
control_var <- subset(control_var, BG != 0 & BG < 1000)
control_var <- subset(control_var, BP != 0 & BP < 1000)
control_var <- subset(control_var, bmi != 0 & bmi < 1000)
control_var <- subset(control_var, age != 0 & age < 1000)
control_var <- subset(control_var, fib != 0 & fib < 10000)

# Assuming case_var and control_var are your dataframes
result_df <- data.frame(
  Column = character(),
  Mean_Case_Var = numeric(),
  Mean_Control_Var = numeric(),
  P_Value = numeric(),
  T_Statistic = numeric(),
  stringsAsFactors = FALSE
)

# Loop through each column
for (col in names(case_var)[-ncol(case_var)]) {  # Exclude the last column
  
  # Perform t-test
  t_test_result <- t.test(case_var[[col]], control_var[[col]])
  
  # Calculate mean values
  mean_case_var <- mean(case_var[[col]])
  mean_control_var <- mean(control_var[[col]])
  
  # Append results to the dataframe
  result_df <- rbind(result_df, data.frame(
    Column = col,
    Mean_Case_Var = mean_case_var,
    Mean_Control_Var = mean_control_var,
    P_Value = t_test_result$p.value,
    T_Statistic = t_test_result$statistic
  ))
}

# Print the result dataframe
as_tibble(result_df)

all_var = rbind(case_var, control_var) %>% 
    mutate_all(as.numeric) %>%
    mutate(PAD = as.integer(PAD))
head(all_var)


