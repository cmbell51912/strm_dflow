from faker import Faker

def new_message():
    fake = Faker()

    profile_result = fake.profile(fields='name,address,sex,job,company')
    message = str(profile_result)

    return(message)