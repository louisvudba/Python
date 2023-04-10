import os
import fileinput
import shutil

def main():
    sourcePath = 'E:\Tmp\Connector\\noti'
    windows_line_ending = b'\r\n'
    linux_line_ending = b'\n'
    dbList = [1,2,3,4,5,6,8,9,10,11,12,13,14,17,18,19,20,21,23,24,26,28,29,30,32,35,36,37]

    arr = os.listdir(sourcePath)    
    for item in arr:
        for db in dbList:
            newFile = item.replace("_0_",f"_{db}_")
            oldFilePath = os.path.join(sourcePath,item)
            newFilePath = os.path.join(sourcePath,newFile)            

            isSucceed = False
            try:
                shutil.copy(oldFilePath,newFilePath)
                isSucceed = True
                #print("File copied successfully.")        
            except shutil.SameFileError:
                print("Source and destination represents the same file.")
                break
            except PermissionError:
                print("Permission denied.")
                break
            except:
                print("Error occurred while copying file.")
                break

            if (isSucceed):
                with fileinput.FileInput(newFilePath, inplace=True, backup=None) as file:
                    for line in file:
                        print(line.replace("_0", f"_{db}"), end='')
                with open(newFilePath, 'rb') as f:
                    content = f.read()
                    content = content.replace(windows_line_ending, linux_line_ending)

                with open(newFilePath, 'wb') as f:
                    f.write(content)

if __name__ == "__main__":
    main()