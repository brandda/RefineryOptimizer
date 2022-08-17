@ECHO OFF

if exist C:\Teradata\Current\General\RefineryOptimizer\Data\inventory_dcd.csv (
	"C:\ProgramData\Anaconda3\python.exe" "c:\teradata\current\general\refineryoptimizer\scripts\python\ro_inventory_dcd_load.py"
) else (
	echo "Input File Does Not Exist in Source Directory"
)

pause


