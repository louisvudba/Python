def one():    
    print(1)
    return "January"
def two():    
    print(2)
    return "February"
def three():  return "March" 

def Choice(itype):
    switcher = {
            "clean": one,
            "text": two,
            "unit": three
        }
    return switcher.get(itype, lambda: "Invalid Month")() 

Choice("text")