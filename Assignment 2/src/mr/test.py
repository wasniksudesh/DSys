f=open("../main/pg-being_ernest.txt")

count=0;
for line in f:
	count+= line.split().count("Author:")


f=open("../main/pg-dorian_gray.txt")

for line in f:
	count+= line.split().count("Author:")

f=open("../main/pg-frankenstein.txt")

for line in f:
	count+= line.split().count("Author:")


f=open("../main/pg-grimm.txt")

for line in f:
	count+= line.split().count("Author:")


f=open("../main/pg-huckleberry_finn.txt")

for line in f:
	count+= line.split().count("Author:")


f=open("../main/pg-metamorphosis.txt")

for line in f:
	count+= line.split().count("Author:")


f=open("../main/pg-sherlock_holmes.txt")

for line in f:
	count+= line.split().count("Author:")



f=open("../main/pg-tom_sawyer.txt")

for line in f:
	count+= line.split().count("Author:")


print(count)