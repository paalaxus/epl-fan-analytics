#!/bin/bash

# Auto Git Push Script
# Use: ./push_to_git.sh "commit message"

# 1. Go to project directory (optional: update this path)
PROJECT_DIR="$HOME/Downloads/EPL_Pipeline"
cd "$PROJECT_DIR" || exit

echo "--------------------------------------------------"
echo "📌 Git Auto-Push Script"
echo "📂 Repository: $PROJECT_DIR"
echo "--------------------------------------------------"

# 2. Check if inside a git repo
if [ ! -d ".git" ]; then
    echo "❌ Not a git repository!"
    exit 1
fi

# 3. Commit message
if [ -z "$1" ]; then
    echo "Enter a commit message:"
    read COMMIT_MSG
else
    COMMIT_MSG="$1"
fi

# 4. Show changes
echo "🔍 Checking status..."
git status

echo
echo "--------------------------------------------------"
echo "🚀 Staging files..."
git add .

# 5. Commit
echo "📝 Committing..."
git commit -m "$COMMIT_MSG"

# 6. Get current branch
BRANCH=$(git rev-parse --abbrev-ref HEAD)

echo "🌿 Current branch: $BRANCH"

# 7. Push
echo "⬆️  Pushing to GitHub..."
git push origin "$BRANCH"

echo "--------------------------------------------------"
echo "✅ Done! Everything is pushed to GitHub."
echo "--------------------------------------------------"

