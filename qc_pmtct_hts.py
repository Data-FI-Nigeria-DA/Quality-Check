import os
import pandas as pd
from datetime import datetime

folder_path ='C:/Users/oluwabukola.arowolo/OneDrive - Palladium International, LLC/Documents/DataFi/FY26Q1_RADET/NEW'

output_base_dir = 'C:/Users/oluwabukola.arowolo/OneDrive - Palladium International, LLC/Documents/DataFi/PMTCT_Project_Export_Quality_Check'
os.makedirs(output_base_dir, exist_ok=True)

all_files = [os.path.join(folder_path,f) for f in os.listdir(folder_path) if f.endswith(('csv','xlsx','.xls'))]

filter_date = datetime(2024, 10, 1)

combined_data = {}

for file in all_files:
    try:
        if file.endswith('csv'):
            data = pd.read_csv(file, encoding = 'latin1', on_bad_lines='skip')
        elif file.endswith('xlsx'):
            data = pd.read_excel(file, engine='openpyxl')
        elif file.endswith('xls'):
            data = pd.read_excel(file)
        else:
            continue

        data['Filename'] = os.path.basename(file)
        combined_data = pd.concat([combined_data, data], ignore_index=True)
    except Exception as e:
        print(f"Error processing file {file}: {e}")

if combined_data.empty():
    print('No file to combine')
    exit()

combined_data['Project_name'] = combined_data['Filename'].str.split('_').str[0]

#convert each date column to date
for col in ['Recency Test Date (yyyy_mm_dd)', 'Date Tested for HIV', 'Mother ART Start Date', 'Mother Date  of Birth', 
            'Viral Load Confirmation Date (yyyyy-mm-dd)', 'Viral Load Sample Collection Date', 'Date Of Maternal Retesting']:
    combined_data[col] = pd.to_datetime(combined_data[col], errors='coerce')

#iterate through each project
for project_name in combined_data['Project_name'].unique():
    project_data = combined_data[combined_data['Project_name']==project_name].copy()

    project_dir = os.path.join(output_base_dir, project_name)
    os.makedirs(project_dir, exist_ok=True)

    project_issues = {}
    all_line_lists_data = []

    #blank ANC Number
    blank_ANC_Number = project_data[
        (project_data['ANC Number'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_ANC_Number'] = blank_ANC_Number.shape[0]
    blank_ANC_Number['Quality_Issue'] = 'blank ANC Number'
    all_line_lists_data.append(blank_ANC_Number)

    #blank Date of Birth
    blank_date_of_birth = project_data[
        (project_data['Mother Date  of Birth'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_date_of_birth'] = blank_date_of_birth.shape[0]
    blank_date_of_birth['Quality_Issue'] = 'blank date of birth'
    all_line_lists_data.append(blank_date_of_birth)

    #blank Sex
    blank_Sex = project_data[
        (project_data['Sex'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_Sex'] = blank_Sex.shape[0]
    blank_Sex['Quality_Issue'] = 'blank Sex'
    all_line_lists_data.append(blank_Sex)

    #blank Marital Status
    blank_Marital_Status = project_data[
        (project_data['Marital Status'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_Marital_Status'] = blank_Marital_Status.shape[0]
    blank_Marital_Status['Quality_Issue'] = 'blank Marital Status'
    all_line_lists_data.append(blank_Marital_Status)

    #blank ANC Setting where ANC Number is not blank
    blank_ANC_Setting_where_ANC_Number_is_not_blank = project_data[
        (project_data['ANC Setting'].isna()) &
        (project_data['ANC Number'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_ANC_Setting_where_ANC_Number_is_not_blank'] = blank_ANC_Setting_where_ANC_Number_is_not_blank.shape[0]
    blank_ANC_Setting_where_ANC_Number_is_not_blank['Quality_Issue'] = 'blank ANC Setting where ANC Number is not blank'
    all_line_lists_data.append(blank_ANC_Setting_where_ANC_Number_is_not_blank)

    #blank Point of Entry
    blank_Point_of_Entry = project_data[
        (project_data['Point of Entry'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_Point_of_Entry'] = blank_Point_of_Entry.shape[0]
    blank_Point_of_Entry['Quality_Issue'] = 'blank Point of Entry'
    all_line_lists_data.append(blank_Point_of_Entry)

    #blank Modality
    blank_Modality = project_data[
        (project_data['Modality'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_Modality'] = blank_Modality.shape[0]
    blank_Modality['Quality_Issue'] = 'blank Modality'
    all_line_lists_data.append(blank_Modality)

    #blank Gestational Age (Weeks) @ First ANC visit
    blank_Gestational_Age_at_First_ANC_visit = project_data[
        (project_data['Gestational Age (Weeks) @ First ANC visit'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_Gestational_Age_at_First_ANC_visit'] = blank_Gestational_Age_at_First_ANC_visit.shape[0]
    blank_Gestational_Age_at_First_ANC_visit['Quality_Issue'] = 'blank Gestational Age (Weeks) @ First ANC visit'
    all_line_lists_data.append(blank_Gestational_Age_at_First_ANC_visit)

    #blank Garvida
    blank_Garvida = project_data[
        (project_data['Garvida'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_Garvida'] = blank_Garvida.shape[0]
    blank_Garvida['Quality_Issue'] = 'blank Garvida'
    all_line_lists_data.append(blank_Garvida)

    #blank Parity
    blank_Parity = project_data[
        (project_data['Parity'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_Parity'] = blank_Parity.shape[0]
    blank_Parity['Quality_Issue'] = 'blank Parity'
    all_line_lists_data.append(blank_Parity)

    #blank Date Tested for HIV
    blank_Date_Tested_for_HIV = project_data[
        (project_data['Date Tested for HIV'].isna()) #&
        #(project_data['Date Tested for HIV'] > filter_date)
    ]
    project_issues['blank_Date_Tested_for_HIV'] = blank_Date_Tested_for_HIV.shape[0]
    blank_Date_Tested_for_HIV['Quality_Issue'] = 'blank Date Tested for HIV'
    all_line_lists_data.append(blank_Date_Tested_for_HIV)

    #blank HIV Test Result
    blank_HIV_Test_Result = project_data[
        (project_data['HIV Test Result'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_HIV_Test_Result'] = blank_HIV_Test_Result.shape[0]
    blank_HIV_Test_Result['Quality_Issue'] = 'blank HIV Test Result'
    all_line_lists_data.append(blank_HIV_Test_Result)

    #blank Recency Test Type where If Recency Testing Opt In is True
    blank_Recency_Test_Type_where_If_Recency_Testing_Opt_In_is_True = project_data[
        (project_data['If Recency Testing Opt In'] =='True') &
        (project_data['Recency Test Type'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_Recency_Test_Type_where_If_Recency_Testing_Opt_In_is_True'] = blank_Recency_Test_Type_where_If_Recency_Testing_Opt_In_is_True.shape[0]
    blank_Recency_Test_Type_where_If_Recency_Testing_Opt_In_is_True['Quality_Issue'] = 'blank Recency Test Type where If Recency Testing Opt In is True'
    all_line_lists_data.append(blank_Recency_Test_Type_where_If_Recency_Testing_Opt_In_is_True)

    #blank Recency Test Date where If Recency Testing Opt In is True
    blank_Recency_Test_Date_where_If_Recency_Testing_Opt_In_is_True = project_data[
        (project_data['If Recency Testing Opt In'] =='True') &
        (project_data['Recency Test Date (yyyy_mm_dd)'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_Recency_Test_Date_where_If_Recency_Testing_Opt_In_is_True'] = blank_Recency_Test_Date_where_If_Recency_Testing_Opt_In_is_True.shape[0]
    blank_Recency_Test_Date_where_If_Recency_Testing_Opt_In_is_True['Quality_Issue'] = 'blank Recency Test Date where If Recency Testing Opt In is True'
    all_line_lists_data.append(blank_Recency_Test_Date_where_If_Recency_Testing_Opt_In_is_True)

    #blank Recency Interpretation where If Recency Testing Opt In is True
    blank_Recency_Interpretation_where_If_Recency_Testing_Opt_In_is_True = project_data[
        (project_data['If Recency Testing Opt In'] =='True') &
        (project_data['Recency Interpretation'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_Recency_Interpretation_where_If_Recency_Testing_Opt_In_is_True'] = blank_Recency_Interpretation_where_If_Recency_Testing_Opt_In_is_True.shape[0]
    blank_Recency_Interpretation_where_If_Recency_Testing_Opt_In_is_True['Quality_Issue'] = 'blank Recency Interpretation where If Recency Testing Opt In is True'
    all_line_lists_data.append(blank_Recency_Interpretation_where_If_Recency_Testing_Opt_In_is_True)

    #blank Final Recency Result where VL Confirmation Result is not blank
    blank_Final_Recency_Result_where_VL_Confirmation_Result_is_not_blank = project_data[
        (~project_data['Viral Load Confirmation Result'].isna()) &
        (project_data['Final Recency Result'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_Final_Recency_Result_where_VL_Confirmation_Result_is_not_blank'] = blank_Final_Recency_Result_where_VL_Confirmation_Result_is_not_blank.shape[0]
    blank_Final_Recency_Result_where_VL_Confirmation_Result_is_not_blank['Quality_Issue'] = 'blank Final Recency Result where VL Confirmation Result is not blank'
    all_line_lists_data.append(blank_Final_Recency_Result_where_VL_Confirmation_Result_is_not_blank)

    #blank Viral Load Confirmation Date where VL Confirmation Result is not blank
    blank_Viral_Load_Confirmation_Date_where_VL_Confirmation_Result_is_not_blank = project_data[
        (~project_data['Viral Load Confirmation Result'].isna()) &
        (project_data['Viral Load Confirmation Date (yyyyy-mm-dd)'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_Viral_Load_Confirmation_Date_where_VL_Confirmation_Result_is_not_blank'] = blank_Viral_Load_Confirmation_Date_where_VL_Confirmation_Result_is_not_blank.shape[0]
    blank_Viral_Load_Confirmation_Date_where_VL_Confirmation_Result_is_not_blank['Quality_Issue'] = 'blank Viral Load Confirmation Date where VL Confirmation Result is not blank'
    all_line_lists_data.append(blank_Viral_Load_Confirmation_Date_where_VL_Confirmation_Result_is_not_blank)

    #blank HIV Test Result where Previously Known Hiv Status is negative
    blank_HIV_Test_Result_where_Previously_Known_Hiv_Status_is_negative = project_data[
        (project_data['Previously Known Hiv Status'] == 'Negative') &
        (project_data['HIV Test Result'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_HIV_Test_Result_where_Previously_Known_Hiv_Status_is_negative'] = blank_HIV_Test_Result_where_Previously_Known_Hiv_Status_is_negative.shape[0]
    blank_HIV_Test_Result_where_Previously_Known_Hiv_Status_is_negative['Quality_Issue'] = 'blank HIV Test Result where Previously Known Hiv Status is negative'
    all_line_lists_data.append(blank_HIV_Test_Result_where_Previously_Known_Hiv_Status_is_negative)

    #blank Previously Known Hiv Status where Date Of Maternal Retesting is not blank
    blank_Previously_Known_Hiv_Status_where_Date_Of_Maternal_Retesting_is_not_blank = project_data[
        (project_data['Previously Known Hiv Status'].isna()) &
        (~project_data['Date Of Maternal Retesting'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_Previously_Known_Hiv_Status_where_Date_Of_Maternal_Retesting_is_not_blank'] = blank_Previously_Known_Hiv_Status_where_Date_Of_Maternal_Retesting_is_not_blank.shape[0]
    blank_Previously_Known_Hiv_Status_where_Date_Of_Maternal_Retesting_is_not_blank['Quality_Issue'] = 'blank Previously Known Hiv Status where Date Of Maternal Retesting is not blank'
    all_line_lists_data.append(blank_Previously_Known_Hiv_Status_where_Date_Of_Maternal_Retesting_is_not_blank)

    #blank Mother Unique ID where Mother ART Start Date is not blank
    blank_Mother_Unique_ID_where_Mother_ART_Start_Date_is_not_blank = project_data[
        (project_data['Mother Unique ID'].isna()) &
        (~project_data['Mother ART Start Date'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_Mother_Unique_ID_where_Mother_ART_Start_Date_is_not_blank'] = blank_Mother_Unique_ID_where_Mother_ART_Start_Date_is_not_blank.shape[0]
    blank_Mother_Unique_ID_where_Mother_ART_Start_Date_is_not_blank['Quality_Issue'] = 'blank Mother Unique ID where Mother ART Start Date is not blank'
    all_line_lists_data.append(blank_Mother_Unique_ID_where_Mother_ART_Start_Date_is_not_blank)

    #Mother ART Start Date less than Mother Date of Birth
    Mother_ART_Start_Date_less_than_Mother_Date_of_Birth = project_data[
        (project_data['Mother ART Start Date'] < project_data['Mother Date  of Birth']) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['Mother_ART_Start_Date_less_than_Mother_Date_of_Birth'] = Mother_ART_Start_Date_less_than_Mother_Date_of_Birth.shape[0]
    Mother_ART_Start_Date_less_than_Mother_Date_of_Birth['Quality_Issue'] = 'Mother ART Start Date less than Mother Date of Birth'
    all_line_lists_data.append(Mother_ART_Start_Date_less_than_Mother_Date_of_Birth)

    #blank Hepatitis B Test Result where Date Tested for Hepatitis B is not blank
    blank_Hepatitis_B_Test_Result_where_Date_Tested_for_Hepatitis_B_is_not_blank = project_data[
        (project_data['Hepatitis B Test Result'].isna()) &
        (~project_data['Date Tested for Hepatitis B'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_Hepatitis_B_Test_Result_where_Date_Tested_for_Hepatitis_B_is_not_blank'] = blank_Hepatitis_B_Test_Result_where_Date_Tested_for_Hepatitis_B_is_not_blank.shape[0]
    blank_Hepatitis_B_Test_Result_where_Date_Tested_for_Hepatitis_B_is_not_blank['Quality_Issue'] = 'blank Hepatitis B Test Result where Date Tested for Hepatitis B is not blank'
    all_line_lists_data.append(blank_Hepatitis_B_Test_Result_where_Date_Tested_for_Hepatitis_B_is_not_blank)

    #blank Hepatitis C Test Result where Date Tested for Hepatitis C is not blank
    blank_Hepatitis_C_Test_Result_where_Date_Tested_for_Hepatitis_C_is_not_blank = project_data[
        (project_data['Hepatitis C Test Result'].isna()) &
        (~project_data['Date Tested for Hepatitis C'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_Hepatitis_C_Test_Result_where_Date_Tested_for_Hepatitis_C_is_not_blank'] = blank_Hepatitis_C_Test_Result_where_Date_Tested_for_Hepatitis_C_is_not_blank.shape[0]
    blank_Hepatitis_C_Test_Result_where_Date_Tested_for_Hepatitis_C_is_not_blank['Quality_Issue'] = 'blank Hepatitis C Test Result where Date Tested for Hepatitis C is not blank'
    all_line_lists_data.append(blank_Hepatitis_C_Test_Result_where_Date_Tested_for_Hepatitis_C_is_not_blank)

    #Recency Test Date less than Date Tested for HIV
    Recency_Test_Date_less_than_Date_Tested_for_HIV = project_data[
        (project_data['Recency Test Date (yyyy_mm_dd)'] < project_data['Date Tested for HIV']) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['Recency_Test_Date_less_than_Date_Tested_for_HIV'] = Recency_Test_Date_less_than_Date_Tested_for_HIV.shape[0]
    Recency_Test_Date_less_than_Date_Tested_for_HIV['Quality_Issue'] = 'Recency Test Date less than Date Tested for HIV'
    all_line_lists_data.append(Recency_Test_Date_less_than_Date_Tested_for_HIV)

    #Viral Load Confirmation Date less than Viral Load Sample Collection Date
    Viral_Load_Confirmation_Date_less_than_Viral_Load_Sample_Collection_Date = project_data[
        (project_data['Viral Load Confirmation Date (yyyyy-mm-dd)'] < project_data['Viral Load Sample Collection Date']) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['Viral_Load_Confirmation_Date_less_than_Viral_Load_Sample_Collection_Date'] = Viral_Load_Confirmation_Date_less_than_Viral_Load_Sample_Collection_Date.shape[0]
    Viral_Load_Confirmation_Date_less_than_Viral_Load_Sample_Collection_Date['Quality_Issue'] = 'Viral Load Confirmation Date less than Viral Load Sample Collection Date'
    all_line_lists_data.append(Viral_Load_Confirmation_Date_less_than_Viral_Load_Sample_Collection_Date)

    #blank Viral Load Sample Collection Date where HIV Test Result is positive and Recency Interpretation is RTRI Recent
    blank_VL_Sample_Collection_Date_where_HIV_Test_Result_is_positive_and_Recency_Interpretation_is_RTRI_Recent = project_data[
        (project_data['Viral Load Sample Collection Date'].isna()) &
        (project_data['Recency Interpretation'] == 'RTRI Recent') &
        (project_data['HIV Test Result'] == 'Positive') &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_VL_Sample_Collection_Date_where_HIV_Test_Result_is_positive_and_Recency_Interpretation_is_RTRI_Recent'] = blank_VL_Sample_Collection_Date_where_HIV_Test_Result_is_positive_and_Recency_Interpretation_is_RTRI_Recent.shape[0]
    blank_VL_Sample_Collection_Date_where_HIV_Test_Result_is_positive_and_Recency_Interpretation_is_RTRI_Recent['Quality_Issue'] = 'blank Viral Load Sample Collection Date where HIV Test Result is positive and Recency Interpretation is RTRI Recent'
    all_line_lists_data.append(blank_VL_Sample_Collection_Date_where_HIV_Test_Result_is_positive_and_Recency_Interpretation_is_RTRI_Recent)


    #blank Maternal Retesting Result where Date Of Maternal Retesting is not blank
    blank_Maternal_Retesting_Result_where_Date_Of_Maternal_Retesting_is_not_blank = project_data[
        (project_data['Maternal Retesting Result'].isna()) &
        (~project_data['Date Of Maternal Retesting'].isna()) &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_Maternal_Retesting_Result_where_Date_Of_Maternal_Retesting_is_not_blank'] = blank_Maternal_Retesting_Result_where_Date_Of_Maternal_Retesting_is_not_blank.shape[0]
    blank_Maternal_Retesting_Result_where_Date_Of_Maternal_Retesting_is_not_blank['Quality_Issue'] = 'blank Maternal Retesting Result where Date Of Maternal Retesting is not blank'
    all_line_lists_data.append(blank_Maternal_Retesting_Result_where_Date_Of_Maternal_Retesting_is_not_blank)


    #blank Mother ART Start Date where HIV Test Result and Maternal Retesting Result is positive 
    blank_Mother_ART_Start_Date_where_HIV_Test_Result_and_Maternal_Retesting_Result_is_positive = project_data[
        (project_data['Mother ART Start Date'].isna()) &
        (project_data['Maternal Retesting Result'] == 'Positive') &
        (project_data['HIV Test Result'] == 'Positive') &
        (project_data['Date Tested for HIV'] >= filter_date)
    ]
    project_issues['blank_Mother_ART_Start_Date_where_HIV_Test_Result_and_Maternal_Retesting_Result_is_positive'] = blank_Mother_ART_Start_Date_where_HIV_Test_Result_and_Maternal_Retesting_Result_is_positive.shape[0]
    blank_Mother_ART_Start_Date_where_HIV_Test_Result_and_Maternal_Retesting_Result_is_positive['Quality_Issue'] = 'blank Mother ART Start Date where HIV Test Result and Maternal Retesting Result is positive'
    all_line_lists_data.append(blank_Mother_ART_Start_Date_where_HIV_Test_Result_and_Maternal_Retesting_Result_is_positive)
    



    # Create DataFrame for transposed quality issues summary for the current project
    quality_issues_df_transposed = pd.DataFrame.from_dict(project_issues, orient='index', columns= ['Number of Records']).reset_index()
    quality_issues_df_transposed.columns = ['Quality Issue', 'Number of Records']

    # Combine all line list data for the current project into one DataFrame
    all_line_lists_df = pd.concat(all_line_lists_data, ignore_index=True)

    # Define the output file path for the current project
    output_file_path =os.path.join(project_dir, f"{project_name}_Quality Checks.xlsx")

    # Save the transposed quality issues and the combined line list to one Excel file with two sheets
    with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
        quality_issues_df_transposed.to_excel(writer, sheet_name="Quality Issues Summary", index=False)
        all_line_lists_df.to_excel(writer, sheet_name= 'Quality Issues Linelist', index=False)

    print(f"Quality check for project '{project_name}' saved to: {output_file_path}")