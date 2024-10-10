# mvp_ballots_all
  - Delete special characters: *, &, "".
  - Standardize player names: delete space, dots, and any other characters. i.e. Acuna Jr. -> AcunaJr, C Davis -> DavisC.
  - Make sure that the name for each distinct nominee is consistant. i.e. Davis -> DavisC, GonzalezA -> Gonzalez.
  - Change all the Latin letters to English letters. i.e. Í to I. 

# mvp_ballots_by_year
  - split the entire_data by league and year, with structure unchanged.

# mvp_candidates_all
  - Make a list of all names in col 1st to 10th
  - Sort list alphabetical
  - Delete duplicates

# mvp_candidates_by_year
  - Serve as auxiliary file
  - mvp_nominees_2012_al.csv ...
  - mvp_voters_2012_al.csv ...

# make_data_list.py
  - utilize entire_data to make separate_data

# make_name_list.py
  - utilize entire_data to produce all_names and separate_names