import re
from pyspark.sql import functions as F

community_names_li =['ALI', 'BAGUM', 'BEGAM', 'BEGUM', 'BHAI', 'BHEN', 'CHARY', 'CHAUDHARY', 'CHOUDHARY',
                     'CHOUDHURY', 'CTTTEST', 'DAS', 'DESHMUKH', 'DEVI', 'GOUD', 'GUPTA', 'IYER', 'KAUR', 
                     'KHAN', 'KUMAR', 'KUMARI', 'LAL', 'LATE', 'MAHAMOOD', 'MD', 'MD.', 'MDBEGAM', 'MDBEGAMMDBEGAMMDBEGAM',
                     'MDBEGUM', 'MDBEGUMMDBEGUMMDBEGUM', 'MENON', 'MOHAMMAD', 'MOHAMMED', 'MOHD', 'MOHEMMED', 'NAIDU', 
                     'NAIR', 'PAL', 'PANDEY', 'PATEL', 'RAI', 'RAO', 'REDDY', 'SAHU', 'SAYEED', 'SHAIK', 'SHARMA', 
                     'SHRILAL', 'SINGH', 'SINGHKAUR', 'SINGHS', 'SRI', 'SRINIVASAN', 'SSINGH', 'SWAMI', 'SWAMY', 
                     'SYED', 'VERMA', 'YADAV']

corp_names_li = ['AGENCY', 'AND', 'APPROVED','APSRTC ', 'AVAILABILITY', 'CENTRAL', 'CO-OPERATIVE', 'COMPANIES', 'CONTRACTS',
                 'CORPORATION', 'EDITION', 'ENTERPRISES', 'EQUITY', 'FIRMS', 'GOVT','GOV', 'GOVT-OWNED', 'GROUP', 'HOLDINGS',
                 'INITIATIVES', 'INVESTMENTS', 'LIABILITY', 'LIMITED', 'LTD', 'MISCELLANEOUS:', 'OFFER', 'OWNERSHIP',
                 'PARTNERSHIP', 'PRIVATE', 'PROJECTS', 'PUBLIC', 'PVT', 'RESOURCES', 'SECTOR', 'SECTOR:', 'SERVICES',
                 'STATE', 'STOCK', 'TIME', 'VENTURES', 'WARRANTY']

common_name = lambda x,li: True if True in[re.sub('[^a-zA-Z ]', ' ',x.upper()).strip() in li for x in x.split(' ')] else False 
single_word_pattern = r'^\w+$'
single_word = lambda x: True if re.findall(single_word_pattern,x.upper()) != [] else False
repeat_char_pattern =  r'(\w)\1{2,}'
repeat_char = lambda x: True  if re.findall(repeat_char_pattern, x.upper()) != [] else False
repeat_Words_pattern =  r'\b(\w+)\b\s+\b\1\b'
repeat_Words = lambda x: True if re.findall(repeat_Words_pattern, x.upper()) != [] else False
number_Words = lambda x: True if re.findall(r'\d', x.upper()) != [] else False


def name_validation(word):
    word = word.strip() if word is not None else word
    if  word is None or word == '':
        
        return 'N/A'
    elif single_word(word) and len(word)<4:
        return 'Single_Word_lessthan4_letters'
    elif single_word(word):
        return 'Single_Word'
    elif common_name(word,community_names_li) and common_name(word,corp_names_li) :
        return 'String_with_Community & Corporate_Name'
    elif common_name(word,community_names_li) :
        return 'String_with_Community_Name'
    elif common_name(word,corp_names_li) :
        return 'String_with_Corporate_Name'
    elif repeat_char(word):
        return "String_with_Repeated_Character"
    elif repeat_Words(word):
        return "String_with_Repeated_Words"
    elif number_Words(word):
        return "String_with_Numbers"
    else:
        return "Valid"
    
def phone_validation(phone_number):
    std_pattern = r'^(\+?91)\s?\d{10}$'
    std_phone = lambda x: True if re.findall(std_pattern, x) != [] else False

    landline_pattrn = r'(.*)([0-9]{3})([^0-9]{1,4})([0-9]{8}$)'
    landline_phone = lambda x: True if re.findall(landline_pattrn, x) != [] else False

    appended_pattern = r'(.*)(\d{15})(.*)'
    appended_phone = lambda x: True if re.findall(appended_pattern, x) != [] else False

    more_len_pattern = r'(.*)(\d{11})'
    more_len_phone = lambda x: True if re.findall(more_len_pattern, x) != [] else False

    invalid_char_pattern = r'[^\d+]'  # This pattern matches any character that is not a digit or '+'
    invalid_char_phone = lambda x: True if re.findall(invalid_char_pattern, x) != [] else False

    if phone_number is None or phone_number == '' or phone_number == 'NA'  :
        return 'N/A'
    elif invalid_char_phone(phone_number):
        return "Contains_Invalid_Characters"
    elif std_phone(phone_number):
        return "Contained_Std_Code"
    elif landline_phone(phone_number):
        return "Land_Line_No"
    elif len(phone_number) < 10:
        return "Length_Less_than_10"
    elif re.findall(r'\W', phone_number) != []:
        res = re.findall(r'\W', phone_number)
        spl_char = [x for x in res if x != '+']
        if spl_char != []:
            spl_char = spl_char[0]
            ph_li = [more_len_phone(x) for x in phone_number.split(spl_char)]
            if len(ph_li) > 1:
                if True in (ph_li):
                    return "Appended_Phone_with_STD_code"
                else:
                    return "Appended_Phone"
    elif appended_phone(phone_number):
        return "Appended_Phone"
    else:
        return "Valid"

def email_validation(email):  
    if  email is None or email == '' or email =='NA' or email =='N/A':
        return 'N/A'
    elif re.match(r"[^@]+@[^@]+\.[^@]+", email):  
        return "Valid"  
    else:
        return "Not Valid"  
def dob_validation(dob):  
    if  dob is None or dob == '' or dob =='NA' or dob =='N/A':
        return 'N/A'
    elif re.match(r"^01\W01\W\d{4}", dob):  
        return "DOB_With: 01-01"  
    elif re.match(r"^01\W0[67]\W\d{4}", dob):  
        return "DOB_With: 01-06 or 01-07"  
    elif re.match(r"^(?:0[1-9]|[12]\d|3[01])([\/.-])(?:0[1-9]|1[012])\1(?:19|20)\d\d$", dob):  
        return "Valid"  
    else:
        return "Not Valid"
    
def Pincode_validation(pincode):
    if  pincode is None or pincode in ['','NA','N/A','NULL',' ','null']:
        return 'N/A'
    elif re.search('[~\!@#=\$%\^&\*\_\+{}()\":;.\'\[\]]',pincode):
        return 'Having_Special_Char'
    elif re.search('[a-zA-Z]{1}',pincode):
        return 'Having_Alphabet'
    elif len(pincode.replace(' ',''))<6:
        return 'Invalid_Having_less_Length'
    elif re.search('\d{7}',pincode):
        return 'Having_More_Length'
    elif re.search('\d{3}\W?\d{3}$',pincode):
        return 'Valid'
    else:
        return 'Invalid'
    
def address_validation(address):
    if  address is None or address in ['','NA','N/A','NULL',' ','null']:
        return 'N/A'
    elif len(address.replace(' ',''))<10:
        return 'Address_Having_less_Length_10'
    elif len(address.replace(' ',''))<15:
        return 'Address_Having_less_Length_15'
    elif re.findall('\d',address) ==[]:
        return 'Address_Without_Number'
    else:
        return 'Valid'
    
    
name_fn  = F.udf(lambda x: name_validation(x))
phone_fn = F.udf(lambda x: phone_validation(x))
email_fn = F.udf(lambda x: email_validation(x))
dob_fn   = F.udf(lambda x: dob_validation(x))
pincode_fn   = F.udf(lambda x: Pincode_validation(x))
address_fn   = F.udf(lambda x: address_validation(x))



