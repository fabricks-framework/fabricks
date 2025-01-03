if [[ "$OSTYPE" == "linux-gnu"* ]] 
then
    source ./.venv/bin/activate
elif [[ "$OSTYPE" == "darwin23" ]]
then
    source ./.venv/bin/activate
else
    source ./.venv/Scripts/activate
fi

while getopts :f:p:s:y:g:c:a option
do
    case $option in
        a) all="true";;
        r) ruff="true";;
        p) python="true";;
        s) sql="true";;
        y) yaml="true";;
        g) git="true";;
    esac
done

if [ -z "$all" ]
then
    all="false"
fi

if [ -z "$ruff" ]
then
    ruff="true"
fi

if [ -z "$python" ]
then
    python="true"
fi

if [ -z "$sql" ]
then
    sql="false"
fi

if [ -z "$yaml" ]
then
    yaml="false"
fi

if [ -z "$git" ]
then
    git="true"
fi

if [ "$all" = "true" ] || [ "$python" = "true" ]
then
    echo "🌟 autoflake(ing) 🌟"
    uv run autoflake -r -i .
    echo "🌟 isort(ing) 🌟"
    uv run isort .
    echo "🌟 pycln(ing) 🌟"
    uv run pycln .
fi

if [ "$all" = "true" ] || [ "$ruff" = "true" ] || [ "$python" = "true" ]
then
    echo "🧙 ruff(ing) 🧙"
    uv run ruff format .
    uv run ruff check . --fix
fi

if [ "$all" = "true" ] || [ "$python" = "true" ]
then
    echo "🌟 pyright(ing) 🌟"
    uv run pyright .
fi

if [ "$all" = "true" ] || [ "$sql" = "true" ]
then
    echo "🌟 sqlfmt(ing) 🌟"
    uv run sqlfmt .
fi

if [ "$all" = "true" ] || [ "$yaml" = "true" ]
then
    echo "🌟 yamlfix(ing) 🌟"
    uv run yamlfix . --exclude .venv --exclude .dev --exclude .idea --include *.yml
fi

if [ "$all" = "true" ] || [ "$git" = "true" ]
then
    echo "🌟 git add(ing) 🌟"
    git add .
fi
