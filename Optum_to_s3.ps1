##This code take's Optum's daily census files, moves to processed, unzips, and pushes to s3
##This code is on a time-schedular in AM


Write-Host "Hello, How Are You?"
#only do for census

$new_date = (Get-Date).ToString('yyyy_MM_dd')
$New_Folder_path = "D:\sftpusers\optum\from-optum\Processed\$((Get-Date).ToString('yyyy_MM_dd'))\"
$New_Folder_path_txt = "D:\sftpusers\optum\from-optum\Processed\$((Get-Date).ToString('yyyy_MM_dd'))\"
$New_Folder_path_zip = "D:\sftpusers\optum\from-optum\Processed\$((Get-Date).ToString('yyyy_MM_dd'))\*.zip"
$destination = New-Item -ItemType Directory -Path $New_Folder_path



#For All Files
Get-ChildItem -Path "D:\sftpusers\optum\from-optum\*.zip"  | Move-Item -Destination  $New_Folder_path -Force -Verbose
#For Just Census
#Get-ChildItem -Path "D:\sftpusers\optum\from-optum\*axial_adt.txt*"  | Move-Item -Destination  $New_Folder_path -Force -Verbose
Get-ChildItem $New_Folder_path_zip | Expand-Archive -DestinationPath $New_Folder_path -Force


aws s3 cp  $New_Folder_path s3://axialhealthcare-knox-etl-raw/client_files/uhctn/From_Remote_Desktop/ --recursive --include "*.txt" --exclude "*deidentified*" --exclude "*.zip"
aws s3 cp  $New_Folder_path s3://axialhealthcare-knox-etl-raw/client_files/uhctn_di/From_Remote_Desktop/ --recursive --exclude "*" --include "*deidentified*" --exclude "*.zip"

Write-Host "Yes, we have the juice"
