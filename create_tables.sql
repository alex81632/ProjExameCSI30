CREATE TABLE  info_cadastro(
    cnpj_fundo  VARCHAR(20),
    denom_social VARCHAR(100),
    tp_fundo VARCHAR(15),
    sit VARCHAR(100),
    CONSTRAINT info_cadastro_pk PRIMARY KEY (cnpj_fundo)
);

CREATE TABLE tempo (
    tempo_id INT,
    dt_comptc TIMESTAMP,
    CONSTRAINT tempo_pk PRIMARY KEY (tempo_id)
);

CREATE TABLE alvo (
    alvo_id BIGSERIAL PRIMARY KEY,
    vl_cota DECIMAL(27, 12),
    vl_total DECIMAL(17, 2),
    vl_patrim_liq DECIMAL(17, 2),
    nr_cotst INT,
    captc_dia DECIMAL(17, 2),
    resg_dia DECIMAL(17, 2),
    tempo_id INT,
    cnpj_fundo VARCHAR(20)
);

ALTER TABLE alvo
ADD CONSTRAINT alvo_tempo_fk FOREIGN KEY (tempo_id) REFERENCES tempo(tempo_id);

ALTER TABLE alvo
ADD CONSTRAINT alvo_info_cadastro_fk FOREIGN KEY (cnpj_fundo) REFERENCES info_cadastro(cnpj_fundo);
