# -----------------------------------------------------------
# Part 1: Acquisition and Flexible Formatting
# -----------------------------------------------------------

import csv
import json

# -----------------------------------------------------------
# Task 1: Create the Tabular CSV Data (Requires Cleaning)
# -----------------------------------------------------------

students_data = [
    # student_id | major | GPA | is_cs_major | credits_taken
    [101, "Computer Science", 3, "Yes", "15.0"],   
    [102, "Statistics", 3.7, "No", "12.5"],
    [103, "Physics", 2, "No", "10.0"],  
    [104, "English", 3.95, "No", "9.5"],
    [105, "Computer Science", 4, "Yes", "11.0"],      
]

# Define the headers for the CSV file
headers = ["student_id", "major", "GPA", "is_cs_major", "credits_taken"]

# Write the data to raw_survey_data.csv
with open("raw_survey_data.csv", "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(headers)
    writer.writerows(students_data)

print("raw_survey_data.csv created successfully")


# -----------------------------------------------------------
# Task 2: Create the Hierarchical JSON Data (Requires Normalization)
# -----------------------------------------------------------

# Define a list of courses with nested instructor info
courses = [
    {
        "course_id": "DS2002",
        "section": "001",
        "title": "Data Science Systems",
        "level": 200,
        "instructors": [
            {"name": "Austin Rivera", "role": "Primary"},
            {"name": "Heywood Williams-Tracy", "role": "TA"}
        ]
    },
    {
        "course_id": "APMA3120",
        "section": "001",
        "title": "Statistics",
        "level": 300,
        "instructors": [
            {"name": "Jim Lark", "role": "Primary"},
        ]
    },
    {
        "course_id": "SYS3021",
        "section": "001",
        "title": "Deterministic Decision Models",
        "level": 300,
        "instructors": [
            {"name": "Robert Riggs", "role": "Primary"}
        ]
    },
    {
        "course_id": "SYS3023",
        "section": "001",
        "title": "Human Machine Interface",
        "level": 300,
        "instructors": [
            {"name": "Sara Riggs", "role": "Primary"}
        ]
    },
    {
        "course_id": "SYS3055",
        "section": "001",
        "title": "Systems Engineering Colloquium I",
        "level": 300,
        "instructors": [
            {"name": "Robert Riggs", "role": "Primary"}
        ]
    },
    {
        "course_id": "SYS3501",
        "section": "001",
        "title": "Programming for Information Engineering",
        "level": 300,
        "instructors": [
            {"name": "Laura Barnes", "role": "Primary"},
            {"name": "Matthew Bolton", "role": "Primary"}
        ]
    },
]

# Write the data to raw_course_catalog.json
with open("raw_course_catalog.json", "w") as jsonfile:
    json.dump(courses, jsonfile, indent=4)

print("raw_course_catalog.json created successfully")


# -----------------------------------------------------------
# Part 2: Data Validation and Type Casting
# -----------------------------------------------------------

import pandas as pd

# -----------------------------------------------------------
# Task 3: Clean and Validate the CSV Data
# -----------------------------------------------------------

df = pd.read_csv("raw_survey_data.csv")

print("\n--- Raw Survey Data ---")
print(df)

# Convert 'is_cs_major' from strings ("Yes"/"No") to Booleans (True/False)
df["is_cs_major"] = df["is_cs_major"].replace({"Yes": True, "No": False})

# Convert GPA and credits_taken to numeric (float) types
df = df.astype({
    "GPA": "float64",
    "credits_taken": "float64"
})

print("\n--- Cleaned Survey Data ---")
print(df.dtypes)  # show column data types

# Save cleaned DataFrame to new CSV
df.to_csv("clean_survey_data.csv", index=False)

print("clean_survey_data.csv created successfully")


# -----------------------------------------------------------
# Task 4: Normalize the JSON Data
# -----------------------------------------------------------

# Load the raw JSON course data
with open("raw_course_catalog.json", "r") as jsonfile:
    course_data = json.load(jsonfile)

# Normalize the nested JSON structure
# Each instructor gets its own row, with course info repeated
df_courses = pd.json_normalize(
    course_data,
    record_path=["instructors"],
    meta=["course_id", "title", "level"]
)

print("\n--- Normalized Course Data ---")
print(df_courses)

# Save normalized DataFrame to new CSV
df_courses.to_csv("clean_course_catalog.csv", index=False)

print("clean_course_catalog.csv created successfully")


