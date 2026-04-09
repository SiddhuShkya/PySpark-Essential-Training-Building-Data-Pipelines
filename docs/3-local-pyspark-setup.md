## Local PySpark Setup (Optional)

This reference provides a brief overview of how to run PySpark locally instead of in a Colab notebook. You will not require this for the course, but this will be helpful when you get started with local development.

**Prerequisites:**

- Familiarity with the terminal application
- Familiarity with using Python on your local machine
- A virtual environment manager for Python development: for example, Conda
- A package manager such as Homebrew on your local machine

---

### Step 1. Install correct Java version 

At the time of writing, PySpark is only compatible with versions 8, 11, and 17 of Java. To check your version of Java, open a terminal and type the following: 

```sh
java --version
```

If the output shows a version starting with 8, 11, or 17, you are all set. Otherwise, you will need to change your version of Java using Homebrew. Run the following in your terminal: 

```sh
brew install openjdk@17
```

You also need to make sure that PySpark can find Java through the JAVA_HOME environment variable. 

Type the following in your terminal to check what JAVA_HOME is set to: 

```sh
echo $JAVA_HOME
```

This should be pointing to the path of the Java installation. If not, you can set the environment variable as follows, assuming you're using the zsh shell on macOS:

```sh
echo export "JAVA_HOME='/opt/homebrew/opt/openjdk@17/'" >> ~/.zshrc
```

### Step 2: Create virtual Python environment

Let's create a new virtual environment for our PySpark development using Conda. In your terminal, type the following:

```sh
conda create --name pyspark-env python=3.9
```

Follow the yes/no prompts to complete the setup. Activate the virtual environment:

```sh
conda activate pyspark-env
```

### Step 3: Install PySpark and Jupyter

You can now install PySpark into your virtual environment using the pip package manager:

```sh
pip install pyspark 
```

Jupyter Notebook provides a convenient interface to using PySpark, so let's install it too:

```sh
pip install jupyter
```

### Step 4: Run PySpark in a Jupyter notebook

Once everything is installed, you can open a new notebook to start writing code. This is how you start up a Jupyter notebook from your terminal:

```sh
jupyter notebook
```

This will open a new browser tab with the Jupyter with a directory listing. Navigate to the directory where you want to store your code. To create a new notebook, click the New button in the top right corner and select Python 3.

You can now test out your PySpark installation in the notebook. Write this in the first code cell and execute it:

```sh
import pyspark
pyspark.__version__
```

You will see the PySpark version output below the code cell.

Next, set the local IP so that Spark knows that you're running all code on your local machine. Execute this in the next code cell:

```sh
%env SPARK_LOCAL_IP=127.0.0.1
```

Finally, you can start a SparkSession in your Jupyter notebook and print some metadata about the SparkSession object:

```python
# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession - master('local') means the SparkSession
# is run locally (vs distributed) on your machine with one worker thread
spark = SparkSession.builder.getOrCreate()
spark
```

This should run correctly and print out some metadata about your SparkSession. For any troubleshooting with your local PySpark installation, please refer to the [official documentation](https://spark.apache.org/docs/latest/api/python/getting_started/install.html).

---

# <div align="center">Thank You for Going Through This Guide! 🙏✨</div>