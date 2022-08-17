@ECHO OFF

if exist C:\Teradata\Current\General\RefineryOptimizer\Data\tag_container_xref.csv (
	"C:\ProgramData\Anaconda3\python.exe" "c:\teradata\current\general\refineryoptimizer\scripts\python\ro_tag_container_xref_load.py"
) else (
	echo "Input File Does Not Exist in Source Directory"
)

pause


