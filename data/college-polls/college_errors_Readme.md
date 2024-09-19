Some suggested changes

1. So, for the raw_data, there are more csvs than there should be, how should we handle that?
-For example, 2023 only has 16 weeks, so week 16 is left empty as a csv
-For 2024, there are only 4 weeks and hence 13 empty weeks
2. For, processed_data, the ranked team lists are also of form 17 weeks so that then means that we are drawing data from thin air
-so basically same errors in raw leading to processed data errors
3. there are no column names on the original csv and also the season csvs
4. found solution for above problem just add (header=True)
-ran into issues though when ran on my system
5. Also, reformatted files so that first script is housed within college-polls and named correctly