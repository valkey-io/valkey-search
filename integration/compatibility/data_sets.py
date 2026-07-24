import itertools, valkey, json, struct, random

### Reusable Data ###
#
# This is the generate data set for all tests.
# Also, common defines.
#
NUM_KEYS = 10
VECTOR_DIM = 3

SETS_KEY = lambda key_type: f"{key_type} sets"
CREATES_KEY = lambda key_type: f"{key_type} creates"

# Text data configuration
TEXT_SCHEMA = {
    'text': ['title', 'body'],
    'tag': ['color'],
    'numeric': ['price']
}

TEXT_DATASETS = {
    'pure text': {
        'schema': TEXT_SCHEMA,
        'field_values': {
            'title': [
                # fruits
                'apple', 'banana', 'orange', 'grape', 'cherry', 'mango',
                # other foods
                'pear', 'peach', 'plum', 'melon', 'kiwi', 'lemon',
                # objects
                'table', 'chair', 'desk', 'lamp', 'window', 'door',
                # abstract / misc
                'music', 'movie', 'book', 'story', 'game', 'puzzle',
                # adjectives
                'quick', 'bright', 'silent', 'heavy', 'smooth', 'sharp'
            ],
            'body': [
                # veggies
                'potato', 'tomato', 'lettuce', 'onion', 'carrot', 'broccoli',
                # animals
                'dog', 'cat', 'horse', 'tiger', 'eagle', 'shark',
                # places
                'city', 'village', 'forest', 'desert', 'ocean', 'river',
                # actions
                'run', 'jump', 'swim', 'drive', 'fly', 'build',
                # descriptors
                'fast', 'slow', 'loud', 'quiet', 'warm', 'cold'
            ],

            'color': [
                'red',
                'yellow',
                'green',
                'purple',
                'blue',
                'black',
                'white',
                'orange',
                'pink',
                'brown'
            ],
            'price': (0, 50)
        }
    },
    'pure text small': {
        'schema': TEXT_SCHEMA,
        'field_values': {
            'title': [
                'apple', 'banana', 'orange', 'grape', 'cherry',
                'dog', 'cat', 'horse', 'city', 'forest'
            ],
            'body': [
                'apple', 'banana', 'orange', 'grape', 'cherry',
                'dog', 'cat', 'horse', 'city', 'forest'
            ],
            'color': ['red', 'yellow', 'green', 'purple', 'blue'],
            'price': (0, 10)
        }
    },
    'numeric text': {
        'schema': TEXT_SCHEMA,
        'field_values': {
            'title': [
                'version',
                '404',
                '+5',
                '-3',
                '3.14',
                '-0.5',
                '10',
                '-2',
                '+1.5',
                'beta'
            ],
            'body': [
                'counter',
                '42',
                '+8',
                '-1',
                '0.75',
                '-2.25',
                'temp',
                '-0.1',
                'gain',
                'loss'
            ],
            'color': [
                'red', 'yellow', 'green', 'purple', 'blue',
                'black', 'white', 'orange', 'pink', 'brown'
            ],
            'price': (0, 50)
        }
    },
    # ,.<>{}[]"':;!@#$%^&*()-+=~
    'punctuation': {
        'schema': TEXT_SCHEMA,
        'field_values': {
            'title': [
                # Unescaped only - these split into multiple tokens
                "comma,period",
                'run.jump',
                'book<paper',
                'physics>maths',
                'cat{dog',
                'fish}rabbit',
                'old[new',
                'tall]short',
                'many"few',
                "great'wall",
                'inside:out',
                'swim;pass',
                'shout!out',
                'email@password',
                'office#home',
                'dollar$sign',
                'ten%percent',
                'top^down',
                'left&right',
                'star*moon',
                'include(exclude',
                'key)board',
                'minus-subtract',
                'city+village',
                'equal=lity',
                'random~sum',
            ],
            'body': [
                'freedom\\,justice',
                'begin\\.end',
                'ask\\<question',
                'get\\>answer',
                'round\\{about',
                'ever\\}green',
                'square\\[feet',
                'circle\\]triangle',
                'chat\\"gpt',
                'redis\\\'valkey',
                'sick\\:hungry',
                'phone\\;laptop',
                'soccer\\!tennis',
                'address\\@field',
                'hash\\#tag',
                'money\\$rich',
                'degree\\%cold',
                'sharp\\^knife',
                'friend\\&enemy',
                'mountain\\*view',
                'extra\\(time',
                'sooner\\)later',
                'deal\\-coupon',
                'abundant\\+plant',
                'blue\\=planet',
                'milky\\~way',
            ],
            'color': ['red', 'blue', 'green'],
            'price': (0, 10)
        }
    }
}

# Multi-language text datasets for compatibility testing.
#
# These datasets are used by the compat suite to generate random queries that
# should produce identical results between Valkey Search and RediSearch. To
# achieve this, the vocabulary is curated to avoid known behavioral differences:
#
# Divergences handled at the vocab level:
#
# 1. Snowball 3.0.1 vs 2.1.0 (Dutch only)
#    Valkey uses Kraaij-Pohlmann which strips ge-/be-/ver- prefixes. RediSearch
#    is observed to use Porter-style stemming which does not strip these.
#    Excluded specific Dutch terms whose K-P stems collide with other dataset
#    terms (e.g. geschilderd, schilder, gebouwd).
#
# 2. CaseMap::utf8Fold decomposition (affects all languages)
#    Valkey's ICU case folding converts ß→ss, ﬁ→fi, and similar. RediSearch is
#    observed to use simple lowercasing which preserves these characters. Avoided
#    characters like ß and ligatures in all language datasets.
#
# 3. Default stop words (affects all languages)
#    Valkey applies per-language Lucene stop words by default. RediSearch is
#    observed to only filter English stop words. Avoided any term that appears
#    in the language's Lucene stop word list.
#
# 4. NOSTEM query-time stemming bypass (affects all languages)
#    When running bare (non-field) queries against RediSearch on NOSTEM fields,
#    stemmed matches are observed (suggesting query-time stemming bypasses
#    NOSTEM). Valkey strictly enforces NOSTEM at both index and query time.
#    Avoided vocab pairs where stem(term_A) == term_B within the same dataset
#    (e.g. kekuatan stems to kuat in Indonesian).
#
# 5. Fuzzy search over stemmed forms (affects all languages, handled at generation time)
#    When running fuzzy queries against RediSearch, matches are observed against
#    stemmed forms of indexed terms in addition to originals. Valkey only matches
#    against original forms. The fuzzy generator filters vocab at generation time
#    to words whose 1-char mutations cannot land within edit-distance 1 of any
#    stem (see _compute_safe_fuzzy_vocab in generate_text.py).
#
# Each dataset includes vocabulary that exercises language-specific features:
# - Diacritics and special characters for normalization/case-folding
# - Inflected forms to exercise Snowball stemming
# - Script-specific characters where applicable
# Word lists are ~30 words per field, organized by semantic category,
# with no overlap between title and body fields.
TEXT_DATASETS_MULTILANG = {
    'french text': {
        'schema': TEXT_SCHEMA,
        'field_values': {
            'title': [
                # food/drink — accents, cedilla
                'café', 'crème', 'gâteau', 'pâtisserie', 'bière', 'fromage',
                # places — circumflex, accents
                'château', 'hôpital', 'forêt', 'île', 'cathédrale', 'musée',
                # people/roles — accents, ligature
                'médecin', 'employé', 'étudiant', 'professeur', 'ingénieur', 'boulanger',
                # nature
                'rivière', 'montagne', 'lumière', 'étoile', 'nuage', 'tempête',
                # verbs (inflected — exercises stemmer)
                'mangeons', 'travaillé', 'chercher', 'finissent', 'commencé', 'découvrir',
            ],
            'body': [
                # objects — accents
                'fenêtre', 'clé', 'vélo', 'échelle', 'réfrigérateur', 'bibliothèque',
                # animals
                'chèvre', 'léopard', 'hérisson', 'écureuil', 'pélican', 'girafe',
                # abstract — accents, circumflex
                'liberté', 'égalité', 'fraternité', 'intérêt', 'beauté', 'vérité',
                # actions (inflected)
                'courir', 'nager', 'réfléchir', 'construire', 'détruire', 'répondre',
                # descriptors — accents
                'rapide', 'lent', 'bruyant', 'silencieux', 'chaud', 'froid',
            ],
            'color': ['rouge', 'bleu', 'vert', 'jaune', 'noir',
                      'blanc', 'violet', 'orange', 'rose', 'brun'],
            'price': (0, 50)
        }
    },
    'german text': {
        'schema': TEXT_SCHEMA,
        'field_values': {
            'title': [
                # places/buildings — umlauts, compounds
                'Gebäude', 'Brücke', 'Universität', 'Küche', 'Bücherei', 'Rathaus',
                # transport — umlauts, compounds
                'Flughafen', 'Führerschein', 'Fahrrad', 'Schiff', 'Eisenbahn', 'Autobahn',
                # people — umlauts
                'Mädchen', 'Ärzte', 'Schüler', 'Händler', 'Bäcker', 'Künstler',
                # nature — umlauts
                'Vögel', 'Bäume', 'Flüsse', 'Gärten', 'Wälder', 'Blüten',
                # verbs (inflected)
                'arbeiten', 'geöffnet', 'verstehen', 'gewünscht', 'übersetzen', 'anfangen',
            ],
            'body': [
                # objects — umlauts
                'Schlüssel', 'Kühlschrank', 'Gemälde', 'Rätsel', 'Bücher', 'Möbel',
                # food
                'Käse', 'Brötchen', 'Würstchen', 'Knödel', 'Gemüse', 'Lebkuchen',
                # abstract — umlauts
                'Stärke', 'Schönheit', 'Fähigkeit', 'Höflichkeit', 'Gemütlichkeit', 'Freiheit',
                # actions (inflected)
                'laufen', 'schwimmen', 'gekämpft', 'geändert', 'zerstört', 'gefahren',
                # descriptors — umlauts
                'schnell', 'langsam', 'böse', 'schön', 'grün', 'müde',
            ],
            'color': ['rot', 'blau', 'grün', 'gelb', 'schwarz',
                      'lila', 'orange', 'rosa', 'braun', 'golden'],
            'price': (0, 50)
        }
    },
    'spanish text': {
        'schema': TEXT_SCHEMA,
        'field_values': {
            'title': [
                # places — tildes, accents
                'ciudad', 'montaña', 'señal', 'estación', 'río', 'jardín',
                # people — ñ, accents
                'niño', 'señor', 'compañero', 'médico', 'músico', 'capitán',
                # food
                'manzana', 'plátano', 'piña', 'limón', 'naranja', 'cereza',
                # nature — accents
                'pájaro', 'corazón', 'árbol', 'océano', 'volcán', 'relámpago',
                # verbs (inflected — exercises stemmer)
                'corriendo', 'trabajaron', 'comiendo', 'vivieron', 'pensando', 'escribió',
            ],
            'body': [
                # objects — accents
                'teléfono', 'lámpara', 'vehículo', 'periódico', 'cámara', 'máquina',
                # animals — accents
                'águila', 'murciélago', 'pingüino', 'delfín', 'tiburón', 'camaleón',
                # abstract — accents
                'información', 'educación', 'solución', 'dirección', 'tradición', 'comunicación',
                # actions (inflected)
                'construir', 'destruir', 'nadar', 'conducir', 'resolver', 'descubrir',
                # descriptors — accents
                'rápido', 'difícil', 'fácil', 'débil', 'útil', 'ágil',
            ],
            'color': ['rojo', 'azul', 'verde', 'amarillo', 'negro',
                      'blanco', 'morado', 'naranja', 'rosa', 'marrón'],
            'price': (0, 50)
        }
    },
    'italian text': {
        'schema': TEXT_SCHEMA,
        'field_values': {
            'title': [
                # places — accented finals
                'città', 'università', 'caffè', 'stazione', 'piazza', 'mercato',
                # people
                'ragazzo', 'dottore', 'professore', 'musicista', 'pittore', 'giornalista',
                # food
                'formaggio', 'pomodoro', 'arancia', 'ciliegia', 'limone', 'fragola',
                # nature — accents
                'montagna', 'temporale', 'fiore', 'albero', 'farfalla', 'nuvola',
                # verbs (inflected — exercises stemmer)
                'mangiando', 'lavorato', 'correre', 'dormire', 'scrivendo', 'costruito',
            ],
            'body': [
                # objects — accents
                'tavolo', 'finestra', 'specchio', 'orologio', 'chiave', 'quaderno',
                # animals
                'gatto', 'cavallo', 'aquila', 'squalo', 'tigre', 'tartaruga',
                # abstract — accents
                'libertà', 'felicità', 'verità', 'società', 'qualità', 'possibilità',
                # actions (inflected)
                'nuotare', 'saltare', 'guidare', 'volare', 'dipingere', 'scoprire',
                # descriptors
                'veloce', 'lento', 'forte', 'debole', 'caldo', 'freddo',
            ],
            'color': ['rosso', 'blu', 'verde', 'giallo', 'nero',
                      'bianco', 'viola', 'arancione', 'rosa', 'marrone'],
            'price': (0, 50)
        }
    },
    'portuguese text': {
        'schema': TEXT_SCHEMA,
        'field_values': {
            'title': [
                # places — cedilla, tildes, accents
                'estação', 'coração', 'ação', 'praça', 'palácio', 'ição',
                # people — tildes, accents
                'capitão', 'irmão', 'médico', 'músico', 'engenheiro', 'professor',
                # food — accents
                'maçã', 'limão', 'pêssego', 'abacaxi', 'morango', 'melão',
                # nature — tildes, accents
                'trovão', 'relâmpago', 'vulcão', 'montanha', 'floresta', 'oceano',
                # verbs (inflected — exercises stemmer)
                'trabalhando', 'correram', 'comendo', 'escreveu', 'construído', 'descobrir',
            ],
            'body': [
                # objects — cedilla, accents
                'televisão', 'geração', 'informação', 'câmera', 'computador', 'relógio',
                # animals — accents
                'tubarão', 'falcão', 'leão', 'camaleão', 'golfinho', 'tartaruga',
                # abstract — tildes, accents
                'educação', 'comunicação', 'tradição', 'solução', 'evolução', 'proteção',
                # actions (inflected)
                'nadar', 'correr', 'voar', 'construir', 'destruir', 'resolver',
                # descriptors — accents
                'rápido', 'difícil', 'fácil', 'possível', 'útil', 'agradável',
            ],
            'color': ['vermelho', 'azul', 'verde', 'amarelo', 'preto',
                      'branco', 'roxo', 'laranja', 'rosa', 'marrom'],
            'price': (0, 50)
        }
    },
    'russian text': {
        'schema': TEXT_SCHEMA,
        'field_values': {
            'title': [
                # places — Cyrillic
                'город', 'деревня', 'площадь', 'станция', 'библиотека', 'больница',
                # people
                'учитель', 'студент', 'инженер', 'художник', 'музыкант', 'писатель',
                # food
                'яблоко', 'молоко', 'хлеб', 'масло', 'сахар', 'картофель',
                # nature
                'дерево', 'река', 'гора', 'облако', 'звезда', 'цветок',
                # verbs (inflected — exercises stemmer)
                'работает', 'бежали', 'читает', 'построил', 'написала', 'открывать',
            ],
            'body': [
                # objects
                'стол', 'окно', 'дверь', 'книга', 'лампа', 'зеркало',
                # animals
                'кошка', 'собака', 'лошадь', 'медведь', 'орёл', 'волк',
                # abstract
                'свобода', 'правда', 'счастье', 'красота', 'мудрость', 'справедливость',
                # actions (inflected)
                'плавать', 'летать', 'строить', 'бегать', 'прыгать', 'водить',
                # descriptors
                'быстрый', 'медленный', 'тихий', 'громкий', 'тёплый', 'холодный',
            ],
            'color': ['красный', 'синий', 'зелёный', 'жёлтый', 'чёрный',
                      'белый', 'фиолетовый', 'оранжевый', 'розовый', 'коричневый'],
            'price': (0, 50)
        }
    },
    'swedish text': {
        'schema': TEXT_SCHEMA,
        'field_values': {
            'title': [
                # places — Å, Ä, Ö
                'sjukhus', 'järnväg', 'övergång', 'flygplats', 'bibliotek', 'slöjd',
                # people — Ö
                'läkare', 'lärare', 'ingenjör', 'författare', 'konstnär', 'hantverkare',
                # food — Ä, Ö
                'äpple', 'smörgås', 'köttbulle', 'räkmacka', 'grädde', 'knäckebröd',
                # nature — Å, Ö
                'ångbåt', 'blåbär', 'björk', 'öken', 'sjö', 'ström',
                # verbs (inflected — exercises stemmer)
                'arbetar', 'öppnade', 'stängde', 'förstår', 'översatte', 'byggde',
            ],
            'body': [
                # objects — Ö, Ä
                'nyckel', 'möbel', 'dörr', 'fönster', 'vägg', 'spegel',
                # animals — Ö
                'häst', 'fågel', 'björn', 'älg', 'räv', 'varg',
                # abstract — Ö, Ä
                'förändring', 'möjlighet', 'rättvisa', 'skönhet', 'säkerhet', 'gemenskap',
                # actions (inflected)
                'simma', 'springa', 'flyga', 'köra', 'bygga', 'måla',
                # descriptors — Å, Ä
                'snabb', 'långsam', 'stark', 'svår', 'varm', 'kall',
            ],
            'color': ['röd', 'blå', 'grön', 'gul', 'svart',
                      'vit', 'lila', 'orange', 'rosa', 'brun'],
            'price': (0, 50)
        }
    },
    'turkish text': {
        'schema': TEXT_SCHEMA,
        'field_values': {
            'title': [
                # places — ş, ç, ğ, ö, ü, ı (dotless i is key for case-fold testing)
                'şehir', 'hastane', 'üniversite', 'köprü', 'çarşı', 'müze',
                # people — ö, ü, ç
                'öğretmen', 'mühendis', 'doçent', 'müdür', 'çiftçi', 'öğrenci',
                # food — ö, ü, ş
                'börek', 'çiçek', 'şeftali', 'üzüm', 'portakal', 'kiraz',
                # nature — dotless ı, ğ, ö
                'ışık', 'dağ', 'göl', 'nehir', 'orman', 'çiçek',
                # verbs (inflected — exercises stemmer + Turkish ı/İ)
                'çalışmak', 'öğrenmek', 'başlamak', 'değiştirmek', 'yürümek', 'düşünmek',
            ],
            'body': [
                # objects — ü, ö, ş, ç
                'anahtar', 'dolap', 'pencere', 'köşe', 'süpürge', 'çamaşır',
                # animals — ş, ç
                'kuş', 'kaplumbağa', 'kelebek', 'karınca', 'köpek', 'maymun',
                # abstract — ö, ü, ğ
                'özgürlük', 'güzellik', 'doğruluk', 'büyüklük', 'güçlülük', 'mutluluk',
                # actions (inflected) — dotless ı testing
                'yüzmek', 'koşmak', 'uçmak', 'sürmek', 'gitmek', 'bulmak',
                # descriptors — ı, ş
                'hızlı', 'yavaş', 'güçlü', 'sessiz', 'sıcak', 'soğuk',
            ],
            'color': ['kırmızı', 'mavi', 'yeşil', 'sarı', 'siyah',
                      'beyaz', 'mor', 'turuncu', 'pembe', 'kahverengi'],
            'price': (0, 50)
        }
    },
    'dutch text': {
        'schema': TEXT_SCHEMA,
        'field_values': {
            'title': [
                # places — IJ digraph, compounds
                'ziekenhuis', 'universiteit', 'bibliotheek', 'vliegveld', 'station', 'wijk',
                # people
                'leraar', 'ingenieur', 'schrijver', 'kunstenaar', 'bakker', 'timmerman',
                # food
                'kaas', 'brood', 'appel', 'sinaasappel', 'citroen', 'aardbei',
                # nature — IJ, compounds
                'ijsbeer', 'rivier', 'bos', 'woestijn', 'bloem', 'storm',
                # verbs (inflected)
                'werken', 'lopen', 'spreken', 'zingen', 'dansen', 'lezen',
            ],
            'body': [
                # objects — compounds
                'sleutel', 'koelkast', 'spiegel', 'boekenkast', 'fiets', 'klok',
                # animals
                'paard', 'vogel', 'vlinder', 'schildpad', 'dolfijn', 'adelaar',
                # abstract
                'vrijheid', 'gelijkheid', 'schoonheid', 'mogelijkheid', 'veiligheid', 'waarheid',
                # actions (inflected)
                'zwemmen', 'rennen', 'vliegen', 'rijden', 'tekenen', 'wandelen',
                # descriptors
                'snel', 'langzaam', 'sterk', 'zwak', 'warm', 'koud',
            ],
            'color': ['rood', 'blauw', 'groen', 'geel', 'zwart',
                      'wit', 'paars', 'oranje', 'roze', 'bruin'],
            'price': (0, 50)
        }
    },
    'indonesian text': {
        'schema': TEXT_SCHEMA,
        'field_values': {
            'title': [
                # places — agglutinative prefixes/suffixes
                'perpustakaan', 'universitas', 'pelabuhan', 'bandara', 'pertokoan', 'perumahan',
                # people — prefixes
                'pelajar', 'pengajar', 'pekerja', 'penulis', 'pelukis', 'penyanyi',
                # food
                'mangga', 'jeruk', 'pisang', 'anggur', 'semangka', 'nanas',
                # nature
                'gunung', 'sungai', 'hutan', 'pantai', 'danau', 'lembah',
                # verbs (inflected — exercises stemmer with me-/ber-/pe- prefixes)
                'membangun', 'berenang', 'berlari', 'menulis', 'membaca', 'memahami',
            ],
            'body': [
                # objects
                'kunci', 'jendela', 'pintu', 'cermin', 'kursi', 'lemari',
                # animals
                'kucing', 'burung', 'kuda', 'harimau', 'elang', 'lumba',
                # abstract — agglutinative suffixes (-an, -kan)
                'keindahan', 'kemerdekaan', 'kebahagiaan', 'kebenaran', 'kesehatan', 'keadilan',
                # actions (inflected)
                'berenang', 'terbang', 'mengemudi', 'melompat', 'memanjat', 'menyelam',
                # descriptors
                'cepat', 'lambat', 'kuat', 'lemah', 'panas', 'dingin',
            ],
            'color': ['merah', 'biru', 'hijau', 'kuning', 'hitam',
                      'putih', 'ungu', 'jingga', 'merah muda', 'cokelat'],
            'price': (0, 50)
        }
    },
    'arabic text': {
        'schema': TEXT_SCHEMA,
        'field_values': {
            'title': [
                # places — Arabic script, various letter forms
                'مدرسة', 'جامعة', 'مستشفى', 'مطار', 'مكتبة', 'متحف',
                # people
                'معلم', 'طبيب', 'مهندس', 'كاتب', 'رسام', 'موسيقي',
                # food
                'تفاحة', 'برتقال', 'عنب', 'ليمون', 'موز', 'فراولة',
                # nature
                'جبل', 'نهر', 'بحر', 'صحراء', 'غابة', 'شجرة',
                # verbs (various forms — tests Arabic morphology)
                'يعمل', 'يكتب', 'يقرأ', 'يبني', 'يفهم', 'يتعلم',
            ],
            'body': [
                # objects
                'مفتاح', 'نافذة', 'باب', 'كتاب', 'مصباح', 'مرآة',
                # animals
                'قطة', 'حصان', 'نسر', 'نمر', 'دلفين', 'سلحفاة',
                # abstract
                'حرية', 'عدالة', 'سعادة', 'جمال', 'حقيقة', 'قوة',
                # actions
                'يسبح', 'يطير', 'يركض', 'يقود', 'يقفز', 'يبحث',
                # descriptors
                'سريع', 'بطيء', 'قوي', 'ضعيف', 'حار', 'بارد',
            ],
            'color': ['أحمر', 'أزرق', 'أخضر', 'أصفر', 'أسود',
                      'أبيض', 'بنفسجي', 'برتقالي', 'وردي', 'بني'],
            'price': (0, 50)
        }
    },
}

# Merge multilang datasets into TEXT_DATASETS so existing code paths work unchanged
TEXT_DATASETS.update(TEXT_DATASETS_MULTILANG)

# Schema flags per field type and schema variant.
# For field types with multiple variants (like "text"), use a dict keyed by schema_type.
# For simple field types, use a plain string (empty string if no extra flags needed).
SCHEMA_FLAGS = {
    "text": {
        "default": "WITHSUFFIXTRIE",
        "nostem": "WITHSUFFIXTRIE NOSTEM",
    },
    "tag": "",
    "numeric": "",
}

def _build_field_schema(field: str, field_type: str, schema_type: str, for_json: bool = False) -> str:
    """Build a single field's schema string for FT.CREATE."""
    flags_entry = SCHEMA_FLAGS[field_type]
    if isinstance(flags_entry, dict):
        if schema_type not in flags_entry:
            raise ValueError(f"Unknown index schema type: {schema_type}")
        flags = flags_entry[schema_type]
    else:
        flags = flags_entry

    field_def = f"{field} {field_type.upper()} {flags}".strip()

    if for_json:
        return f"$.{field} AS {field_def}"
    return field_def

def unbytes(b):
    if isinstance(b, bytes):
        return b.decode("utf-8")
    else:
        return b
class ClientSystem:
    def __init__(self, address):
        self.address = address
        self.client = valkey.Valkey(host=address[0], port=address[1])

    def execute_command(self, *cmd):
        print("Execute:", *cmd)
        result = self.client.execute_command(*cmd)
        #print("Result:", result)
        return result
    
    def ft_info(self, index):
        values = self.client.execute_command(f"FT.INFO {index}")
        result = {unbytes(values[i]):unbytes(values[i+1]) for i in range(0, len(values), 2)}
        return result
        
    def pipeline(self):
        return self.client.pipeline()
    
    def wait_for_indexing_done(self, index_name):
        assert False
    
    def hset(self, *cmd):
        return self.client.hset(*cmd)
    
def array_encode(key_type, array):
    if key_type == "hash":
        return struct.pack(f"<{len(array)}f", *array)
    else:
        return array

def json_quote(s):
    if s == '"':
        return '\\"'
    if s == '\\':
        return '\\\\'
    return f'\\u{s:04x}'

def binary_string_encode(key_type, s):
    if key_type == "hash":
        return s
    else:
        return '"' + "".join([json_quote(s[i]) for i in range(len(s))]) + '"'       
    
def compute_data_sets():
    '''Generate all of the possible data sets'''
    data = {}

    create_cmds = {
        "hash": "FT.CREATE hash_idx1 ON HASH PREFIX 1 hash: SCHEMA {}",
        "json": "FT.CREATE json_idx1 ON JSON PREFIX 1 json: SCHEMA {}",
    }
    field_type_to_name = {"tag": "t", "numeric": "n", "vector": "v"}
    field_types_to_count = {"numeric": 3, "tag": 3, "vector" : 1}

    def make_field_definition(key_type, name, typ, i):
        if typ == "vector":
            if key_type == "hash":
                return f"{name}{i} vector HNSW 6 DIM {VECTOR_DIM} TYPE FLOAT32 DISTANCE_METRIC L2"
            else:
                return f"$.{name}{i} as {name}{i} vector HNSW 6 DIM {VECTOR_DIM} TYPE FLOAT32 DISTANCE_METRIC L2"
            return f"{name}{i} vector HNSW 6 DIM {VECTOR_DIM} TYPE FLOAT32 DISTANCE_METRIC L2"
        else:
            return f"{name}{i} {typ}" if key_type == "hash" else f"$.{name}{i} AS {name}{i} {typ}"

    data["hard numbers"] = {}
    data["sortable numbers"] = {}
    data["reverse vector numbers"] = {}
    data["bad numbers"] = {}
    data["bad vectors"] = {}
    data["hard strings"] = {}
    data["tag special chars"] = {}
    vec_algos = ["flat", "hnsw"]
    metrics = ["cosine", "ip", "l2"]
    for algo in vec_algos:
        for metric in metrics:
            data[f"vector data {metric} {algo}"] = {}
    for key_type in ["hash", "json"]:
        schema = [
            make_field_definition(key_type, field_type_to_name[typ], typ, i + 1)
            for typ, count in field_types_to_count.items()
            for i in range(count)
        ]
        schema = " ".join(schema)
        #
        # Hard Numbers, edge case numbers.
        #
        hard_numbers = [-0.5, 0, -0, 1, -1] # todo "nan", -0
        if key_type == "hash":
            hard_numbers += [float("inf"), float("-inf")]
        combinations = list(itertools.combinations(hard_numbers, 3))
        data["hard numbers"][CREATES_KEY(key_type)] = [create_cmds[key_type].format(schema)]
        data["hard numbers"][SETS_KEY(key_type)] = [
            (
                f"{key_type}:{i:02d}",
                {
                    "n1": combinations[i][0],
                    "n2": combinations[i][1],
                    "n3" : combinations[i][2],
                    "t1": f"one.one{i*2}",
                    "t2": f"two.two{i*-2}",
                    "t3": "all_the_same_value",
                    "v1": array_encode(key_type, [i for _ in range(VECTOR_DIM)]),
                    "e1" : 1,
                    "e2" : "two",
                },
            )
            for i in range(len(combinations))
        ]
        #
        # Sortable numbers, designed so that sorted keys for this index don't have any duplications
        # which makes the compare functions harder.
        #
        sortable_numbers = range(-5, 10)
        data["sortable numbers"][CREATES_KEY(key_type)] = [create_cmds[key_type].format(schema)]
        data["sortable numbers"][SETS_KEY(key_type)] = [
            (
                f"{key_type}:{i:02d}",
                {
                    "n1": sortable_numbers[i],
                    "n2": -sortable_numbers[i],
                    "n3" : sortable_numbers[-i],
                    "t1": f"one.one{i*2}",
                    "t2": f"two.two{i*-2}",
                    "t3": "all_the_same_value",
                    "v1": array_encode(key_type, [i for _ in range(VECTOR_DIM)]),
                    "e1" : 1,
                    "e2" : "two",
                },
            )
            for i in range(len(sortable_numbers))
        ]
        #
        # Sortable numbers, designed so that sorted keys for this index don't have any duplications
        # which makes the compare functions harder.
        #
        sortable_numbers = range(-5, 10)
        data["reverse vector numbers"][CREATES_KEY(key_type)] = [create_cmds[key_type].format(schema)]
        data["reverse vector numbers"][SETS_KEY(key_type)] = [
            (
                f"{key_type}:{i:02d}",
                {
                    "n1": sortable_numbers[i],
                    "n2": -sortable_numbers[i],
                    "n3" : sortable_numbers[-i],
                    "t1": f"one.one{i*2}",
                    "t2": f"two.two{i*-2}",
                    "t3": "all_the_same_value",
                    "v1": array_encode(key_type, [(len(sortable_numbers)-i) for _ in range(VECTOR_DIM)]),
                    "e1" : 1,
                    "e2" : "two",
                },
            )
            for i in range(len(sortable_numbers))
        ]
        #
        #  Bad numbers, things that don't convert to their designated types.
        #
        data["bad numbers"][CREATES_KEY(key_type)] = [create_cmds[key_type].format(schema)]
        data["bad numbers"][SETS_KEY(key_type)] = [
            (f"{key_type}:0",
                {
                    "n1": 0,
                    "n2": 0,
                    "n3": 0,
                    "t1": "",
                    "t2": "",
                    "t3": "",
                    "v1": array_encode(key_type, [0 for _ in range(VECTOR_DIM)]),
                    "e1" : 1,
                    "e2" : "two",
                },
            ),
            (f"{key_type}:1",
                {
                    "n1": "bad",
                    "n2": 0,
                    "n3": 0,
                    "t1": "",
                    "t2": "",
                    "t3": "",
                    "v1": array_encode(key_type, [1 for _ in range(VECTOR_DIM)]),
                    "e1" : 1,
                    "e2" : "two",
                },
            ),
            (f"{key_type}:2",
                {
                    "n1": True if key_type == "json" else 1,
                    "n2": 0,
                    "n3": 0,
                    "t1": "",
                    "t2": "",
                    "t3": "",
                    "v1": array_encode(key_type, [2 for _ in range(VECTOR_DIM)]),
                    "e1" : 1,
                    "e2" : "two",
                },
            ),
            (f"{key_type}:3",
                {
                    # "n1": 0,
                    "n2": 0,
                    "n3": 0,
                    "t1": "",
                    "t2": "",
                    "t3": "",
                    "v1": array_encode(key_type, [3 for _ in range(VECTOR_DIM)]),
                    "e1" : 1,
                    "e2" : "two",
                },
            ),
            (f"{key_type}:4",
                {
                    "n1": 0,
                    "n2": 0,
                    "n3": 0,
                    "t2": "",
                    "t3": "",
                    "v1": array_encode(key_type, [4 for _ in range(VECTOR_DIM)]),
                    "e1" : 1,
                    "e2" : "two",
                },
            ),
            (f"{key_type}:5",
                {
                    "n1": 0,
                    "n2": 0,
                    "n3": 0,
                    "t2": "",
                    "t3": "",
                    "v1": array_encode(key_type, [5 for _ in range(VECTOR_DIM+1)]),
                },
            ),
        ]
        #
        # Bad vectors: every field is valid except the VECTOR field v1, which on
        # some keys has the wrong number of elements (VECTOR_DIM+1 instead of
        # VECTOR_DIM). Redisearch drops the whole key when a vector field is
        # malformed, so those keys must not appear in any query result.
        #
        data["bad vectors"][CREATES_KEY(key_type)] = [create_cmds[key_type].format(schema)]
        data["bad vectors"][SETS_KEY(key_type)] = [
            (f"{key_type}:{i}",
                {
                    "n1": i,
                    "n2": -i,
                    "n3": i * 2,
                    "t1": f"tag{i}",
                    "t2": "common",
                    "t3": "all_the_same_value",
                    # Keys 1 and 3 carry a wrong-length (and therefore invalid)
                    # vector; the rest are valid.
                    "v1": array_encode(
                        key_type,
                        [i for _ in range(VECTOR_DIM + (1 if i in (1, 3) else 0))]),
                    "e1": 1,
                    "e2": "two",
                },
            )
            for i in range(6)
        ]
        #
        # hard strings
        #
        unicode_chars = "".join(
            [chr(c) for c in range(0, 128)] + 
            [chr(c) for c in range(0x7f, 0x82)] +
            [chr(c) for c in range(0x7ff, 0x802)] +
            [chr(c) for c in range(0xFFFB, 0x10002)] +
            [chr(c) for c in range(0x10FFFB, 0x110000)])
        data["hard strings"][CREATES_KEY(key_type)] = [create_cmds[key_type].format(schema)]
        data["hard strings"][SETS_KEY(key_type)] = [
            (
                f"{key_type}:{i:02d}",
                {
                    "n1": 0,
                    "n2": -i,
                    "n3" : i*2,
                    "t1": unicode_chars,
                    "t2": unicode_chars[i:],
                    "t3": "all_the_same_value",
                    "v1": array_encode(key_type, [i for _ in range(VECTOR_DIM)]),
                    "e1" : 1,
                    "e2" : "two",
                },
            )
            for i in range(20)
        ]
        for algo in vec_algos:
            for metric in metrics:
                vector_points = [-1.5, 1.5]
                data[f"vector data {metric} {algo}"][CREATES_KEY(key_type)] = [create_cmds[key_type].format(schema).replace("L2", metric).replace("HNSW", algo)]
                data[f"vector data {metric} {algo}"][SETS_KEY(key_type)] = [
                    (
                        f"{key_type}:{x:}:{y}:{z}",
                        {
                            "n1": x,
                            "n2": y,
                            "n3" : z,
                            "t1": "",
                            "t2": "",
                            "t3": "all_the_same_value",
                            "v1": array_encode(key_type, [x, y, z]),
                            "e1" : 1,
                            "e2" : "two",
                        },
                    )
                    for x in vector_points
                    for y in vector_points
                    for z in vector_points
                ]

    # Tag special characters data set. Comma separator so } and | are literal;
    # avoid '-' etc. (query operators) or the reference engine rejects the query.
    tag_special_base_tags = ["a}b", "a|b", "normal", "x}y}z",
                             "café", "中文", "😀", "a\\b"]
    # Backslash-escaped values, kept out of the comma-separated pairwise combos.
    tag_escape_only_tags = ['a"b', "a\tb", "a\nb"]
    for key_type in ["hash", "json"]:
        # Comma separator (non-default)
        if key_type == "hash":
            create_cmd = ("FT.CREATE hash_idx1 ON HASH PREFIX 1 hash: "
                          "SCHEMA tags TAG SEPARATOR ,")
        else:
            create_cmd = ("FT.CREATE json_idx1 ON JSON PREFIX 1 json: "
                          "SCHEMA $.tags AS tags TAG SEPARATOR ,")

        docs = []
        doc_id = 1
        # Single-tag documents
        for tag in tag_special_base_tags:
            docs.append((f"{key_type}:{doc_id}", {"tags": tag}))
            doc_id += 1
        # Multi-tag combinations (all pairs)
        for t1, t2 in itertools.combinations(tag_special_base_tags, 2):
            docs.append((f"{key_type}:{doc_id}", {"tags": f"{t1},{t2}"}))
            doc_id += 1
        # Escape-only single-tag documents
        for tag in tag_escape_only_tags:
            docs.append((f"{key_type}:{doc_id}", {"tags": tag}))
            doc_id += 1

        data["tag special chars"][CREATES_KEY(key_type)] = [create_cmd]
        data["tag special chars"][SETS_KEY(key_type)] = docs

    return data

# Mapping from dataset name to the LANGUAGE parameter value for FT.CREATE.
# English datasets (original) use "english"; multilang datasets use their language.
DATASET_LANGUAGE_MAP = {
    'pure text': 'english',
    'pure text small': 'english',
    'numeric text': 'english',
    'punctuation': 'english',
    'french text': 'french',
    'german text': 'german',
    'spanish text': 'spanish',
    'italian text': 'italian',
    'portuguese text': 'portuguese',
    'russian text': 'russian',
    'swedish text': 'swedish',
    'turkish text': 'turkish',
    'dutch text': 'dutch',
    'indonesian text': 'indonesian',
    'arabic text': 'arabic',
}


def compute_text_data_sets(dataset_name, seed=123, schema_type="default", language=None):
    """Generate random documents for a specific dataset.
    
    Args:
        dataset_name: Name of dataset (e.g., "pure text", "french text")
        seed: Random seed for reproducibility
        schema_type: Schema variant ("default" or "nostem")
        language: Language for FT.CREATE LANGUAGE option. If None, auto-detected
                  from DATASET_LANGUAGE_MAP (defaults to "english").
    
    Returns:
        dict with structure: {dataset_name: {"hash creates": [...], "hash sets": [...], ...}}
    """
    if dataset_name not in TEXT_DATASETS:
        raise ValueError(f"Unknown dataset: {dataset_name}. Available: {list(TEXT_DATASETS.keys())}")
    
    # Auto-detect language from dataset name if not explicitly provided
    if language is None:
        language = DATASET_LANGUAGE_MAP.get(dataset_name, 'english')
    
    config = TEXT_DATASETS[dataset_name]
    field_values = config['field_values']
    schema = config['schema']
    
    random.seed(seed)
    
    data = {}
    data[dataset_name] = {}
    
    text_fields = schema.get('text', [])
    tag_fields = schema.get('tag', [])
    numeric_fields = schema.get('numeric', [])
    
    # Build create commands for both hash and json, with LANGUAGE option
    language_clause = f"LANGUAGE {language} " if language != "english" else ""
    create_cmds = {
        "hash": f"FT.CREATE hash_idx1 ON HASH PREFIX 1 hash: {language_clause}SCHEMA {{}}",
        "json": f"FT.CREATE json_idx1 ON JSON PREFIX 1 json: {language_clause}SCHEMA {{}}",
    }
    
    # Build schema strings using the shared helper
    hash_schema_parts = []
    json_schema_parts = []

    for field in text_fields:
        hash_schema_parts.append(_build_field_schema(field, "text", schema_type, for_json=False))
        json_schema_parts.append(_build_field_schema(field, "text", schema_type, for_json=True))

    for field in tag_fields:
        hash_schema_parts.append(_build_field_schema(field, "tag", schema_type, for_json=False))
        json_schema_parts.append(_build_field_schema(field, "tag", schema_type, for_json=True))

    for field in numeric_fields:
        hash_schema_parts.append(_build_field_schema(field, "numeric", schema_type, for_json=False))
        json_schema_parts.append(_build_field_schema(field, "numeric", schema_type, for_json=True))

    hash_schema = " ".join(hash_schema_parts)
    json_schema = " ".join(json_schema_parts)
    
    # Get vocab for text fields
    vocab = {}
    for field in text_fields:
        if field in field_values:
            vocab[field] = field_values[field]

    # Helper to generate a document
    def generate_doc(doc_id):
        fields = {}
        # Text fields - generate random length 1-5 for title and body
        for field in text_fields:
            if field in vocab:
                num_words = random.randint(1, 10)
                words = random.choices(vocab[field], k=num_words)
                fields[field] = " ".join(words)
        # Tag fields
        for field in tag_fields:
            if field in field_values:
                fields[field] = random.choice(field_values[field])
        # Numeric fields
        for field in numeric_fields:
            if field in field_values:
                min_val, max_val = field_values[field]
                fields[field] = random.randint(min_val, max_val)
        return fields
    
    for key_type in ["hash", "json"]:
        # Set create commands
        if key_type == "hash":
            data[dataset_name][CREATES_KEY(key_type)] = [create_cmds[key_type].format(hash_schema)]
        else:
            data[dataset_name][CREATES_KEY(key_type)] = [create_cmds[key_type].format(json_schema)]
        
        # Generate documents
        docs = []
        for i in range(NUM_KEYS):
            fields = generate_doc(i)
            docs.append((f"{key_type}:{i:02d}", fields))
        
        data[dataset_name][SETS_KEY(key_type)] = docs
    
    return data

### Helper Functions ###
def load_data(client, data_set, key_type, data_source=None, schema_type="default", language=None):
    # Auto-detect data source based on data_set name
    if data_source is None:
        data_source = "text" if data_set in TEXT_DATASETS else "vector"

    match data_source:
        case "vector":
            data = compute_data_sets()
        case "text":
            data = compute_text_data_sets(data_set, schema_type=schema_type, language=language)
        case _:
            raise ValueError(f"Unknown data source: {data_source}")
    load_list = data[data_set][SETS_KEY(key_type)]
    for create_index_cmd in data[data_set][CREATES_KEY(key_type)]:
        client.execute_command(create_index_cmd)

    # Make large chunks to accelerate things
    batch_size = 50
    for s in range(0, len(load_list), batch_size):
        pipe = client.pipeline()
        for cmd in load_list[s : s + batch_size]:
            if key_type == "hash":
                pipe.hset(cmd[0], mapping=cmd[1])
            else:
                pipe.execute_command(*["JSON.SET", cmd[0], "$", json.dumps(cmd[1])])
        pipe.execute()

    # client.wait_for_indexing_done(f"{key_type}_idx1")
    print(f"setup_data completed {data_set} {key_type}")

    # Print loaded data for debugging
    print(f"Loaded {len(load_list)} items")
    for s in range(min(10, len(load_list))):  # Print first 10 items
        print(f"{s}:{load_list[s][0]}: {load_list[s][1]}")

    if key_type != "hash":
        for s in range(0, len(load_list)):
            k = client.execute_command(*["JSON.GET", load_list[s][0], "$"])
            print(f"{s}:{load_list[s][0]}:  ", k)
    return len(load_list)

def load_data_cluster(cluster_client, test_case, data_set, key_type):
    data = compute_data_sets()

    primary0 = test_case.new_client_for_primary(0)
    for create_cmd in data[data_set][CREATES_KEY(key_type)]:
        primary0.execute_command(create_cmd)

    for key, fields in data[data_set][SETS_KEY(key_type)]:
        if key_type == "hash":
            cluster_client.hset(key, mapping=fields)
        else:
            cluster_client.execute_command(
                "JSON.SET", key, "$", json.dumps(fields)
            )

    print(f"cluster load completed {data_set} {key_type}")

def extract_vocab_from_text_data(dataset_name, key_type):
    """Extract unique words from TEXT fields in a text data set."""
    data = compute_text_data_sets(dataset_name)
    
    vocab = set()
    for _, fields in data[dataset_name][SETS_KEY(key_type)]:
        for field in TEXT_SCHEMA['text']:
            if field in fields:
                words = fields[field].split()
                vocab.update(words)
    return sorted(list(vocab))

def extract_tag_values_from_text_data(dataset_name, key_type):
    """Extract unique tag values from TAG fields in a text data set."""
    data = compute_text_data_sets(dataset_name)
    
    tag_values = {}
    for _, fields in data[dataset_name][SETS_KEY(key_type)]:
        for field in TEXT_SCHEMA['tag']:
            if field in fields:
                if field not in tag_values:
                    tag_values[field] = set()
                tag_values[field].add(fields[field])
    return {k: sorted(list(v)) for k, v in tag_values.items()}

def extract_numeric_ranges_from_text_data(dataset_name, key_type):
    """Extract min/max ranges from NUMERIC fields in a text data set."""
    data = compute_text_data_sets(dataset_name)
    
    numeric_ranges = {}
    for _, fields in data[dataset_name][SETS_KEY(key_type)]:
        for field in TEXT_SCHEMA['numeric']:
            if field in fields:
                value = int(fields[field])
                if field not in numeric_ranges:
                    numeric_ranges[field] = [value, value]
                else:
                    numeric_ranges[field][0] = min(numeric_ranges[field][0], value)
                    numeric_ranges[field][1] = max(numeric_ranges[field][1], value)
    return {k: tuple(v) for k, v in numeric_ranges.items()}

def extract_vocab_by_field_from_text_data(dataset_name, key_type):
    """Extract vocabularies per field from TEXT fields in a text data set.
    
    Returns:
        dict of {field_name: [unique_words]}
    """
    if dataset_name not in TEXT_DATASETS:
        raise ValueError(f"Unknown dataset: {dataset_name}")
    
    config = TEXT_DATASETS[dataset_name]
    field_values = config['field_values']
    schema = config['schema']
    
    vocab_by_field = {}
    for field in schema.get('text', []):
        if field in field_values:
            vocab_by_field[field] = field_values[field]
    
    return vocab_by_field
