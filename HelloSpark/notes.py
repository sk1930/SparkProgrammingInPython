'''We learned to set up our local system for running developing, and running Apache Spark.
In this video, I want to help you create and run your first Spark application using the
PyCharm IDE. So start your PyCharm IDE and create a new project. Set your new project location. You can hit the browse button and choose your working directory. Then type a
new project directory location.
C:\Users\Student\Desktop\Personal\Spark\pythonProject

New environment using VirualEnv
C:\Users\Student\Desktop\Personal\Spark\pythonProject1\venv

base interpreter
Here am selecting python 3.8 but not latest one


in the Python Interpreter dropdown. You might have multiple Python installations on your system. But we want to make sure we choose the latest one that we installed for
use with Spark. Uncheck all other options. We do not need anything else. We are using VirtualEnv for setting up our project. There are other options, but VirtualEnv is the easiest. So we use the VirtualEnv and hit the create button. Wait for a few minutes to let your IDE set up a blank project for you. Done. Now you can create a new Python file in your project directory. We will be using pyspark for coding our spark projects. So make sure you have the pyspark package installed in your project's VirtualEnv. You check that by clicking the python packages window at the bottom of your IDE. You can see my installed packages here. I already have the pyspark package installed.

search for pyspark and
Come to the right side, three dots. I see a delete option here because I have installed it already.

If you do not see it in the list of your installed packages, search for it. Select the pyspark package under the PyPI repository. Come to the right side, three dots. I see a delete option here because I have installed it already. But you may see an install button to install the selected package. Install it if you do not have it already installed. Minimize it once your pyspark package is installed. Now we can start typing the code. We haven't started learning Spark programming yet. So I am not expecting you to understand the code. However, I want to create a super simple Spark application and run it from the IDE So you can watch me coding even if you do not understand the code at this stage. You can also type along and test your IDE setup. I will come back to the IDE in future lessons and explain everything. But for now, let me type some code and run it. So I am printing Hello Spark. It is not a Spark program but just a simple Hello Spark Python code. Let me run it and see if Python is working. It worked. I can see the output here. Now let me write some Spark code. So here is my super simple Spark application. I am creating a Spark Dataframe using a data list. I am giving some column names to list items and executing the show() method on my dataframe. Do not stress yourself to understand the code. Simply type these lines and run it. You should see some warnings, and you can ignore them. Finally, you will see Dataframe output. We managed to run a super simple Spark application on our local computer. That's all we wanted to do. See you again in the following video. Keep Learning and Keep Growing.'''
