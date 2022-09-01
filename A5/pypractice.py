

file = open("Rental.txt", "r")
    
lines =  file.readlines()
for line in lines:
    val = line.split("|") or line.split("\n")
    print(val)
    
print(val[0])