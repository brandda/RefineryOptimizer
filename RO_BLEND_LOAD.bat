@ECHO OFF

if exist C:\Teradata\Current\General\RefineryOptimizer\Data\blend.csv (
	"C:\ProgramData\Anaconda3\python.exe" "c:\current\teradata\general\refineryoptimizer\scripts\python\ro_blend_load.py"
) else (
	echo "Input File Does Not Exist in Source Directory"
)

pause


