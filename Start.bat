title 启动中......
@echo off
cls

:: 检查管理员权限
echo 正在检查管理员权限......
NET SESSION >nul 2>&1
if %errorLevel% == 0 (
    :: 已经是管理员，继续执行脚本
    goto :ELEVATED
) else (
    :: 请求提升为管理员权限
    echo 正在提升管理员权限......
    echo Set UAC = CreateObject^("Shell.Application"^) > "%temp%\getadmin.vbs"
    echo UAC.ShellExecute "%~s0", "", "", "runas", 1 >> "%temp%\getadmin.vbs"
    "%temp%\getadmin.vbs"
    del "%temp%\getadmin.vbs"
    exit /B
)

:ELEVATED
:: 以下是已经以管理员权限运行的代码
echo 已确认为管理员权限。
cls

:: 延迟环境变量
echo 正在设置延迟环境变量......
setlocal enabledelayedexpansion
cls

:: 切换到当前BAT文件所在的真实根目录，不受启动位置影响
echo 正在定位根目录......
cd /d "%~dp0"

goto :MENU

:MENU
title 主菜单
:: color F0
cls
echo 1. 直接开始
echo 2. 自定义开始
echo 3. 使用说明
echo 4. 查看作者
echo 5. 首次/更新初始化
echo 0. 退出程序
echo *输入选项前面的数字即可（不带点）。

set /p choice=请输入选项：

if !choice! == 1 (goto :DEFAULT
) else if !choice! == 2 (goto :CUSTOM
) else if !choice! == 3 (goto :GUIDE
) else if !choice! == 4 (goto :AUTHOR
) else if !choice! == 5 (goto :INITIALIZE
) else if !choice! == 0 (goto :EXIT
) else (goto :ERROR)

:DEFAULT
title 标准模式
python Information-Gatherer.py --run

shutdown -s -t 60
pause
goto :MENU

:CUSTOM
title 自定义模式
set /p mode=请输入模式名称（不带括号及“--”）：
python Information-Gatherer.py --!mode!

shutdown -s -t 60
pause
goto :MENU

:GUIDE
title 使用说明
echo 程序原生使用说明如下：
python Information-Gatherer.py --help
pause
goto :MENU

:AUTHOR
title 作者信息
echo 作者：高远
echo GitHub：GaoYuan522
echo 手机：138 1055 5080
echo 邮箱：GaoYuan13810088206@163.com
echo ——————————————————本信息更新于2026年8月12日——————————————————
pause
cls
goto :MENU

:INITIALIZE
title 初始化
python Information-Gatherer.py --init
pause
goto :MENU

:EXIT
title 即将退出程序
echo 感谢使用本程序。
pause
exit

:ERROR
title 错误
echo 无效选项，请重新输入。
pause
cls
goto :MENU