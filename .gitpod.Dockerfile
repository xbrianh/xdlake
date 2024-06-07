FROM gitpod/workspace-full

RUN sudo apt-get update && sudo apt-get install -y tmux fzf

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    sudo ./aws/install

# set default shell to zsh
RUN sudo chsh -s /bin/zsh gitpod
