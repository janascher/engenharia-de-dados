import pandas as pd

# Dicionário com os dados
student_data = {
    "Nome": ["Lara", "Pablo", "Silvana", "Sebastian"],
    "Nota": [8.5, 7.2, 9.5, 8.6],
}

# DataFrame
student_df = pd.DataFrame(student_data).set_index("Nome")

# Calcula a média da coluna "Nota"
media = student_df["Nota"].mean()

# Imprime todas as informações dos alunos
print(student_df)
print()
# Imprime a média da coluna "Nota"
print(f"Média das notas dos alunos: {media}.")
