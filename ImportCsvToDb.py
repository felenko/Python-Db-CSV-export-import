import os
import glob

path = 'C:\\Users\\kostiantyn.felenko\\PycharmProjects\\untitled'
extension = 'csv'
os.chdir(path)
result = glob.glob('*.{}'.format(extension))
print(result)