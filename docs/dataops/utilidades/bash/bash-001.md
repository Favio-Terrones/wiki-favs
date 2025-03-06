# GitHub Workflow Bash

## Objetivo
Este script Bash automatiza la validación y actualización de workflows en repositorios de GitHub pertenecientes a una organización o usuario específico. Su función es garantizar que los archivos de workflow usen inicialmente `ubuntu-20.04` para luego actualizarlos a `ubuntu-24.04` de forma automática, integrando los cambios en la rama **especificada**.

## Funcionalidades
- **Automatización:** Clona repositorios y procesa cada uno sin intervención manual.
- **Validación:** Verifica que todos los workflows tengan configurado `runs-on: ubuntu-20.04` antes de actualizar.
- **Actualización:** Reemplaza la versión de Ubuntu en los archivos YAML y realiza commit y push a la rama **especificada**.
- **Limpieza:** Elimina los repositorios clonados una vez finalizado el proceso.


## Código del Script

```bash
#!/bin/bash  
# Variables: configuramos nuestro usuario (o nombre de la organización) y, si es necesario, token
USERNAME="Alicorp-Digital"
# Opcional: exportamos el token si GitHub CLI lo requiere, por ejemplo:
# export GITHUB_TOKEN="tu_token"

# Listamos todos los repositorios
repos=$(gh repo list $USERNAME --limit 1000 --json nameWithOwner -q '.[].nameWithOwner')


for repo in $repos; do
  echo "$repo..."
  git clone https://github.com/$repo.git
  repo_dir=$(basename $repo)
  cd "$repo_dir" || continue
  
  # Verificamos que exista el directorio de workflows
  if [ -d ".github/workflows" ]; then

    # Validamos que TODOS los workflows tengan en sus líneas 'runs-on:' solo "ubuntu-20.04"
    valid=true
    for file in .github/workflows/*.yml; do
      if [ -f "$file" ]; then
        # Extraemos las líneas que contengan "runs-on:"
        runs_on_lines=$(grep "runs-on:" "$file")
        if [ -n "$runs_on_lines" ]; then
          # Si alguna de las líneas no contiene "ubuntu-20.04", se marca el repo como no válido
          if echo "$runs_on_lines" | grep -q -v "ubuntu-20.04"; then
             valid=false
             echo "Skipping $repo: $file tiene una línea 'runs-on:' que no es ubuntu-20.04"
             break
          fi
        fi
      fi
    done

    if [ "$valid" = false ]; then
      echo "Skipping repository $repo because not all workflows use exclusively ubuntu-20.04"
      cd ..
      rm -rf "$repo_dir"
      continue
    fi

    # Actualizamos cada archivo .yml dentro de la carpeta workflows
    for file in .github/workflows/*.yml; do
      if [ -f "$file" ]; then
        echo "Actualizando $file..."
        # Reemplazamos versiones antiguas por ubuntu-24.04
        sed -i 's/runs-on: ubuntu-[0-9]\+\(\.[0-9]\+\)\?/runs-on: ubuntu-24.04/g' "$file"
      fi
    done
    
    # Si hubo cambios, los comitea y empuja
    if [ -n "$(git status --porcelain)" ]; then
      git add .github/workflows
      git commit -m "[no ci] Actualización automática: workflow a ubuntu-24.04"
      # Pusheamos a la rama que necesitamos
      git push origin prod
    fi
  else
    echo "No se encontró el directorio .github/workflows en $repo"
  fi
  
  cd ..
  # Limpiamos el repositorio clonado
  rm -rf "$repo_dir"
done

```