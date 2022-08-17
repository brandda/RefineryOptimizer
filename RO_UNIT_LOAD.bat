@ECHO OFF

if exist C:\Teradata\Current\General\RefineryOptimizer\Data\unit.csv (
	"C:\ProgramData\Anaconda3\python.exe" "c:\teradata\current\general\refineryoptimizer\scripts\python\ro_unit_load.py"
) else (
	echo "Input File Does Not Exist in Source Directory"
)

pause


