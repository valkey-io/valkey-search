/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

//
// Per-language default stop word lists.
//
// Stop word lists sourced from Apache Lucene.
// Licensed under the Apache License, Version 2.0.
// https://github.com/apache/lucene
// https://www.apache.org/licenses/LICENSE-2.0
//
// Snowball language stop words from:
//   lucene/analysis/common/src/resources/org/apache/lucene/analysis/snowball/
//
// Arabic stop words from:
//   lucene/analysis/common/src/resources/org/apache/lucene/analysis/ar/
//
// Turkish stop words from:
//   lucene/analysis/common/src/resources/org/apache/lucene/analysis/tr/
//

#ifndef VALKEYSEARCH_SRC_INDEXES_TEXT_STOP_WORDS_H_
#define VALKEYSEARCH_SRC_INDEXES_TEXT_STOP_WORDS_H_

#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/strings/ascii.h"
#include "src/index_schema.pb.h"

namespace valkey_search::indexes::text {

// English stop words (33 words)
inline const std::vector<std::string> kEnglishStopWords{
    "a",    "is",   "the", "an",   "and",  "are",   "as",   "at",    "be",
    "but",  "by",   "for", "if",   "in",   "into",  "it",   "no",    "not",
    "of",   "on",   "or",  "such", "that", "their", "then", "there", "these",
    "they", "this", "to",  "was",  "will", "with"};

// French stop words (154 words)
inline const std::vector<std::string> kFrenchStopWords{
    "au",      "aux",     "avec",     "ce",      "ces",      "dans",
    "de",      "des",     "du",       "elle",    "en",       "et",
    "eux",     "il",      "je",       "la",      "le",       "leur",
    "lui",     "ma",      "mais",     "me",      "même",     "mes",
    "moi",     "mon",     "ne",       "nos",     "notre",    "nous",
    "on",      "ou",      "par",      "pas",     "pour",     "qu",
    "que",     "qui",     "sa",       "se",      "ses",      "sur",
    "ta",      "te",      "tes",      "toi",     "ton",      "tu",
    "un",      "une",     "vos",      "votre",   "vous",     "c",
    "d",       "j",       "l",        "à",       "m",        "n",
    "s",       "t",       "y",        "étée",    "étées",    "étant",
    "suis",    "es",      "êtes",     "sont",    "serai",    "seras",
    "sera",    "serons",  "serez",    "seront",  "serais",   "serait",
    "serions", "seriez",  "seraient", "étais",   "était",    "étions",
    "étiez",   "étaient", "fus",      "fut",     "fûmes",    "fûtes",
    "furent",  "sois",    "soit",     "soyons",  "soyez",    "soient",
    "fusse",   "fusses",  "fussions", "fussiez", "fussent",  "ayant",
    "eu",      "eue",     "eues",     "eus",     "ai",       "avons",
    "avez",    "ont",     "aurai",    "aurons",  "aurez",    "auront",
    "aurais",  "aurait",  "aurions",  "auriez",  "auraient", "avais",
    "avait",   "aviez",   "avaient",  "eut",     "eûmes",    "eûtes",
    "eurent",  "aie",     "aies",     "ait",     "ayons",    "ayez",
    "aient",   "eusse",   "eusses",   "eût",     "eussions", "eussiez",
    "eussent", "ceci",    "cela",     "celà",    "cet",      "cette",
    "ici",     "ils",     "les",      "leurs",   "quel",     "quels",
    "quelle",  "quelles", "sans",     "soi"};

// German stop words (231 words)
inline const std::vector<std::string> kGermanStopWords{
    "aber",     "alle",      "allem",     "allen",     "aller",     "alles",
    "als",      "also",      "am",        "an",        "ander",     "andere",
    "anderem",  "anderen",   "anderer",   "anderes",   "anderm",    "andern",
    "anderr",   "anders",    "auch",      "auf",       "aus",       "bei",
    "bin",      "bis",       "bist",      "da",        "damit",     "dann",
    "der",      "den",       "des",       "dem",       "die",       "das",
    "daß",      "derselbe",  "derselben", "denselben", "desselben", "demselben",
    "dieselbe", "dieselben", "dasselbe",  "dazu",      "dein",      "deine",
    "deinem",   "deinen",    "deiner",    "deines",    "denn",      "derer",
    "dessen",   "dich",      "dir",       "du",        "dies",      "diese",
    "diesem",   "diesen",    "dieser",    "dieses",    "doch",      "dort",
    "durch",    "ein",       "eine",      "einem",     "einen",     "einer",
    "eines",    "einig",     "einige",    "einigem",   "einigen",   "einiger",
    "einiges",  "einmal",    "er",        "ihn",       "ihm",       "es",
    "etwas",    "euer",      "eure",      "eurem",     "euren",     "eurer",
    "eures",    "für",       "gegen",     "gewesen",   "hab",       "habe",
    "haben",    "hat",       "hatte",     "hatten",    "hier",      "hin",
    "hinter",   "ich",       "mich",      "mir",       "ihr",       "ihre",
    "ihrem",    "ihren",     "ihrer",     "ihres",     "euch",      "im",
    "in",       "indem",     "ins",       "ist",       "jede",      "jedem",
    "jeden",    "jeder",     "jedes",     "jene",      "jenem",     "jenen",
    "jener",    "jenes",     "jetzt",     "kann",      "kein",      "keine",
    "keinem",   "keinen",    "keiner",    "keines",    "können",    "könnte",
    "machen",   "man",       "manche",    "manchem",   "manchen",   "mancher",
    "manches",  "mein",      "meine",     "meinem",    "meinen",    "meiner",
    "meines",   "mit",       "muss",      "musste",    "nach",      "nicht",
    "nichts",   "noch",      "nun",       "nur",       "ob",        "oder",
    "ohne",     "sehr",      "sein",      "seine",     "seinem",    "seinen",
    "seiner",   "seines",    "selbst",    "sich",      "sie",       "ihnen",
    "sind",     "so",        "solche",    "solchem",   "solchen",   "solcher",
    "solches",  "soll",      "sollte",    "sondern",   "sonst",     "über",
    "um",       "und",       "uns",       "unse",      "unsem",     "unsen",
    "unser",    "unses",     "unter",     "viel",      "vom",       "von",
    "vor",      "während",   "war",       "waren",     "warst",     "was",
    "weg",      "weil",      "weiter",    "welche",    "welchem",   "welchen",
    "welcher",  "welches",   "wenn",      "werde",     "werden",    "wie",
    "wieder",   "will",      "wir",       "wird",      "wirst",     "wo",
    "wollen",   "wollte",    "würde",     "würden",    "zu",        "zum",
    "zur",      "zwar",      "zwischen"};

// Spanish stop words (308 words)
inline const std::vector<std::string> kSpanishStopWords{
    "de",        "la",         "que",          "el",          "en",
    "y",         "a",          "los",          "del",         "se",
    "las",       "por",        "un",           "para",        "con",
    "no",        "una",        "su",           "al",          "lo",
    "como",      "más",        "pero",         "sus",         "le",
    "ya",        "o",          "este",         "sí",          "porque",
    "esta",      "entre",      "cuando",       "muy",         "sin",
    "sobre",     "también",    "me",           "hasta",       "hay",
    "donde",     "quien",      "desde",        "todo",        "nos",
    "durante",   "todos",      "uno",          "les",         "ni",
    "contra",    "otros",      "ese",          "eso",         "ante",
    "ellos",     "e",          "esto",         "mí",          "antes",
    "algunos",   "qué",        "unos",         "yo",          "otro",
    "otras",     "otra",       "él",           "tanto",       "esa",
    "estos",     "mucho",      "quienes",      "nada",        "muchos",
    "cual",      "poco",       "ella",         "estar",       "estas",
    "algunas",   "algo",       "nosotros",     "mi",          "mis",
    "tú",        "te",         "ti",           "tu",          "tus",
    "ellas",     "nosotras",   "vosotros",     "vosotras",    "os",
    "mío",       "mía",        "míos",         "mías",        "tuyo",
    "tuya",      "tuyos",      "tuyas",        "suyo",        "suya",
    "suyos",     "suyas",      "nuestro",      "nuestra",     "nuestros",
    "nuestras",  "vuestro",    "vuestra",      "vuestros",    "vuestras",
    "esos",      "esas",       "estoy",        "estás",       "está",
    "estamos",   "estáis",     "están",        "esté",        "estés",
    "estemos",   "estéis",     "estén",        "estaré",      "estarás",
    "estará",    "estaremos",  "estaréis",     "estarán",     "estaría",
    "estarías",  "estaríamos", "estaríais",    "estarían",    "estaba",
    "estabas",   "estábamos",  "estabais",     "estaban",     "estuve",
    "estuviste", "estuvo",     "estuvimos",    "estuvisteis", "estuvieron",
    "estuviera", "estuvieras", "estuviéramos", "estuvierais", "estuvieran",
    "estuviese", "estuvieses", "estuviésemos", "estuvieseis", "estuviesen",
    "estando",   "estado",     "estada",       "estados",     "estadas",
    "estad",     "he",         "has",          "ha",          "hemos",
    "habéis",    "han",        "haya",         "hayas",       "hayamos",
    "hayáis",    "hayan",      "habré",        "habrás",      "habrá",
    "habremos",  "habréis",    "habrán",       "habría",      "habrías",
    "habríamos", "habríais",   "habrían",      "había",       "habías",
    "habíamos",  "habíais",    "habían",       "hube",        "hubiste",
    "hubo",      "hubimos",    "hubisteis",    "hubieron",    "hubiera",
    "hubieras",  "hubiéramos", "hubierais",    "hubieran",    "hubiese",
    "hubieses",  "hubiésemos", "hubieseis",    "hubiesen",    "habiendo",
    "habido",    "habida",     "habidos",      "habidas",     "soy",
    "eres",      "es",         "somos",        "sois",        "son",
    "sea",       "seas",       "seamos",       "seáis",       "sean",
    "seré",      "serás",      "será",         "seremos",     "seréis",
    "serán",     "sería",      "serías",       "seríamos",    "seríais",
    "serían",    "era",        "eras",         "éramos",      "erais",
    "eran",      "fui",        "fuiste",       "fue",         "fuimos",
    "fuisteis",  "fueron",     "fuera",        "fueras",      "fuéramos",
    "fuerais",   "fueran",     "fuese",        "fueses",      "fuésemos",
    "fueseis",   "fuesen",     "siendo",       "sido",        "tengo",
    "tienes",    "tiene",      "tenemos",      "tenéis",      "tienen",
    "tenga",     "tengas",     "tengamos",     "tengáis",     "tengan",
    "tendré",    "tendrás",    "tendrá",       "tendremos",   "tendréis",
    "tendrán",   "tendría",    "tendrías",     "tendríamos",  "tendríais",
    "tendrían",  "tenía",      "tenías",       "teníamos",    "teníais",
    "tenían",    "tuve",       "tuviste",      "tuvo",        "tuvimos",
    "tuvisteis", "tuvieron",   "tuviera",      "tuvieras",    "tuviéramos",
    "tuvierais", "tuvieran",   "tuviese",      "tuvieses",    "tuviésemos",
    "tuvieseis", "tuviesen",   "teniendo",     "tenido",      "tenida",
    "tenidos",   "tenidas",    "tened"};

// Italian stop words (279 words)
inline const std::vector<std::string> kItalianStopWords{
    "ad",        "al",       "allo",      "ai",         "agli",     "all",
    "agl",       "alla",     "alle",      "con",        "col",      "coi",
    "da",        "dal",      "dallo",     "dai",        "dagli",    "dall",
    "dagl",      "dalla",    "dalle",     "di",         "del",      "dello",
    "dei",       "degli",    "dell",      "degl",       "della",    "delle",
    "in",        "nel",      "nello",     "nei",        "negli",    "nell",
    "negl",      "nella",    "nelle",     "su",         "sul",      "sullo",
    "sui",       "sugli",    "sull",      "sugl",       "sulla",    "sulle",
    "per",       "tra",      "contro",    "io",         "tu",       "lui",
    "lei",       "noi",      "voi",       "loro",       "mio",      "mia",
    "miei",      "mie",      "tuo",       "tua",        "tuoi",     "tue",
    "suo",       "sua",      "suoi",      "sue",        "nostro",   "nostra",
    "nostri",    "nostre",   "vostro",    "vostra",     "vostri",   "vostre",
    "mi",        "ti",       "ci",        "vi",         "lo",       "la",
    "li",        "le",       "gli",       "ne",         "il",       "un",
    "uno",       "una",      "ma",        "ed",         "se",       "perché",
    "anche",     "come",     "dov",       "dove",       "che",      "chi",
    "cui",       "non",      "più",       "quale",      "quanto",   "quanti",
    "quanta",    "quante",   "quello",    "quelli",     "quella",   "quelle",
    "questo",    "questi",   "questa",    "queste",     "si",       "tutto",
    "tutti",     "a",        "c",         "e",          "i",        "l",
    "o",         "ho",       "hai",       "ha",         "abbiamo",  "avete",
    "hanno",     "abbia",    "abbiate",   "abbiano",    "avrò",     "avrai",
    "avrà",      "avremo",   "avrete",    "avranno",    "avrei",    "avresti",
    "avrebbe",   "avremmo",  "avreste",   "avrebbero",  "avevo",    "avevi",
    "aveva",     "avevamo",  "avevate",   "avevano",    "ebbi",     "avesti",
    "ebbe",      "avemmo",   "aveste",    "ebbero",     "avessi",   "avesse",
    "avessimo",  "avessero", "avendo",    "avuto",      "avuta",    "avuti",
    "avute",     "sono",     "sei",       "è",          "siamo",    "siete",
    "sia",       "siate",    "siano",     "sarò",       "sarai",    "sarà",
    "saremo",    "sarete",   "saranno",   "sarei",      "saresti",  "sarebbe",
    "saremmo",   "sareste",  "sarebbero", "ero",        "eri",      "era",
    "eravamo",   "eravate",  "erano",     "fui",        "fosti",    "fu",
    "fummo",     "foste",    "furono",    "fossi",      "fosse",    "fossimo",
    "fossero",   "essendo",  "faccio",    "fai",        "facciamo", "fanno",
    "faccia",    "facciate", "facciano",  "farò",       "farai",    "farà",
    "faremo",    "farete",   "faranno",   "farei",      "faresti",  "farebbe",
    "faremmo",   "fareste",  "farebbero", "facevo",     "facevi",   "faceva",
    "facevamo",  "facevate", "facevano",  "feci",       "facesti",  "fece",
    "facemmo",   "faceste",  "fecero",    "facessi",    "facesse",  "facessimo",
    "facessero", "facendo",  "sto",       "stai",       "sta",      "stiamo",
    "stanno",    "stia",     "stiate",    "stiano",     "starò",    "starai",
    "starà",     "staremo",  "starete",   "staranno",   "starei",   "staresti",
    "starebbe",  "staremmo", "stareste",  "starebbero", "stavo",    "stavi",
    "stava",     "stavamo",  "stavate",   "stavano",    "stetti",   "stesti",
    "stette",    "stemmo",   "steste",    "stettero",   "stessi",   "stesse",
    "stessimo",  "stessero", "stando"};

// Portuguese stop words (203 words)
inline const std::vector<std::string> kPortugueseStopWords{
    "de",          "a",         "o",
    "que",         "e",         "do",
    "da",          "em",        "um",
    "para",        "com",       "não",
    "uma",         "os",        "no",
    "se",          "na",        "por",
    "mais",        "as",        "dos",
    "como",        "mas",       "ao",
    "ele",         "das",       "à",
    "seu",         "sua",       "ou",
    "quando",      "muito",     "nos",
    "já",          "eu",        "também",
    "só",          "pelo",      "pela",
    "até",         "isso",      "ela",
    "entre",       "depois",    "sem",
    "mesmo",       "aos",       "seus",
    "quem",        "nas",       "me",
    "esse",        "eles",      "você",
    "essa",        "num",       "nem",
    "suas",        "meu",       "às",
    "minha",       "numa",      "pelos",
    "elas",        "qual",      "nós",
    "lhe",         "deles",     "essas",
    "esses",       "pelas",     "este",
    "dele",        "tu",        "te",
    "vocês",       "vos",       "lhes",
    "meus",        "minhas",    "teu",
    "tua",         "teus",      "tuas",
    "nosso",       "nossa",     "nossos",
    "nossas",      "dela",      "delas",
    "esta",        "estes",     "estas",
    "aquele",      "aquela",    "aqueles",
    "aquelas",     "isto",      "aquilo",
    "estou",       "está",      "estamos",
    "estão",       "estive",    "esteve",
    "estivemos",   "estiveram", "estava",
    "estávamos",   "estavam",   "estivera",
    "estivéramos", "esteja",    "estejamos",
    "estejam",     "estivesse", "estivéssemos",
    "estivessem",  "estiver",   "estivermos",
    "estiverem",   "hei",       "há",
    "havemos",     "hão",       "houve",
    "houvemos",    "houveram",  "houvera",
    "houvéramos",  "haja",      "hajamos",
    "hajam",       "houvesse",  "houvéssemos",
    "houvessem",   "houver",    "houvermos",
    "houverem",    "houverei",  "houverá",
    "houveremos",  "houverão",  "houveria",
    "houveríamos", "houveriam", "sou",
    "somos",       "são",       "era",
    "éramos",      "eram",      "fui",
    "foi",         "fomos",     "foram",
    "fora",        "fôramos",   "seja",
    "sejamos",     "sejam",     "fosse",
    "fôssemos",    "fossem",    "for",
    "formos",      "forem",     "serei",
    "será",        "seremos",   "serão",
    "seria",       "seríamos",  "seriam",
    "tenho",       "tem",       "temos",
    "tém",         "tinha",     "tínhamos",
    "tinham",      "tive",      "teve",
    "tivemos",     "tiveram",   "tivera",
    "tivéramos",   "tenha",     "tenhamos",
    "tenham",      "tivesse",   "tivéssemos",
    "tivessem",    "tiver",     "tivermos",
    "tiverem",     "terei",     "terá",
    "teremos",     "terão",     "teria",
    "teríamos",    "teriam"};

// Russian stop words (159 words)
inline const std::vector<std::string> kRussianStopWords{
    "и",      "в",       "во",      "не",      "что",     "он",      "на",
    "я",      "с",       "со",      "как",     "а",       "то",      "все",
    "она",    "так",     "его",     "но",      "да",      "ты",      "к",
    "у",      "же",      "вы",      "за",      "бы",      "по",      "только",
    "ее",     "мне",     "было",    "вот",     "от",      "меня",    "еще",
    "нет",    "о",       "из",      "ему",     "теперь",  "когда",   "даже",
    "ну",     "вдруг",   "ли",      "если",    "уже",     "или",     "ни",
    "быть",   "был",     "него",    "до",      "вас",     "нибудь",  "опять",
    "уж",     "вам",     "сказал",  "ведь",    "там",     "потом",   "себя",
    "ничего", "ей",      "может",   "они",     "тут",     "где",     "есть",
    "надо",   "ней",     "для",     "мы",      "тебя",    "их",      "чем",
    "была",   "сам",     "чтоб",    "без",     "будто",   "человек", "чего",
    "раз",    "тоже",    "себе",    "под",     "жизнь",   "будет",   "ж",
    "тогда",  "кто",     "этот",    "говорил", "того",    "потому",  "этого",
    "какой",  "совсем",  "ним",     "здесь",   "этом",    "один",    "почти",
    "мой",    "тем",     "чтобы",   "нее",     "кажется", "сейчас",  "были",
    "куда",   "зачем",   "сказать", "всех",    "никогда", "сегодня", "можно",
    "при",    "наконец", "два",     "об",      "другой",  "хоть",    "после",
    "над",    "больше",  "тот",     "через",   "эти",     "нас",     "про",
    "всего",  "них",     "какая",   "много",   "разве",   "сказала", "три",
    "эту",    "моя",     "впрочем", "хорошо",  "свою",    "этой",    "перед",
    "иногда", "лучше",   "чуть",    "том",     "нельзя",  "такой",   "им",
    "более",  "всегда",  "конечно", "всю",     "между"};

// Swedish stop words (114 words)
inline const std::vector<std::string> kSwedishStopWords{
    "och",    "det",    "att",    "i",      "en",    "jag",    "hon",
    "som",    "han",    "på",     "den",    "med",   "var",    "sig",
    "för",    "så",     "till",   "är",     "men",   "ett",    "om",
    "hade",   "de",     "av",     "icke",   "mig",   "du",     "henne",
    "då",     "sin",    "nu",     "har",    "inte",  "hans",   "honom",
    "skulle", "hennes", "där",    "min",    "man",   "ej",     "vid",
    "kunde",  "något",  "från",   "ut",     "när",   "efter",  "upp",
    "vi",     "dem",    "vara",   "vad",    "över",  "än",     "dig",
    "kan",    "sina",   "här",    "ha",     "mot",   "alla",   "under",
    "någon",  "eller",  "allt",   "mycket", "sedan", "ju",     "denna",
    "själv",  "detta",  "åt",     "utan",   "varit", "hur",    "ingen",
    "mitt",   "ni",     "bli",    "blev",   "oss",   "din",    "dessa",
    "några",  "deras",  "blir",   "mina",   "samma", "vilken", "er",
    "sådan",  "vår",    "blivit", "dess",   "inom",  "mellan", "sådant",
    "varför", "varje",  "vilka",  "ditt",   "vem",   "vilket", "sitt",
    "sådana", "vart",   "dina",   "vars",   "vårt",  "våra",   "ert",
    "era",    "vilkas"};

// Dutch stop words (101 words)
inline const std::vector<std::string> kDutchStopWords{
    "de",     "en",      "van",   "ik",     "te",     "dat",   "die",
    "in",     "een",     "hij",   "het",    "niet",   "zijn",  "is",
    "was",    "op",      "aan",   "met",    "als",    "voor",  "had",
    "er",     "maar",    "om",    "hem",    "dan",    "zou",   "of",
    "wat",    "mijn",    "men",   "dit",    "zo",     "door",  "over",
    "ze",     "zich",    "bij",   "ook",    "tot",    "je",    "mij",
    "uit",    "der",     "daar",  "haar",   "naar",   "heb",   "hoe",
    "heeft",  "hebben",  "deze",  "u",      "want",   "nog",   "zal",
    "me",     "zij",     "nu",    "ge",     "geen",   "omdat", "iets",
    "worden", "toch",    "al",    "waren",  "veel",   "meer",  "doen",
    "toen",   "moet",    "ben",   "zonder", "kan",    "hun",   "dus",
    "alles",  "onder",   "ja",    "eens",   "hier",   "wie",   "werd",
    "altijd", "doch",    "wordt", "wezen",  "kunnen", "ons",   "zelf",
    "tegen",  "na",      "reeds", "wil",    "kon",    "niets", "uw",
    "iemand", "geweest", "andere"};

// Indonesian stop words (93 words)
inline const std::vector<std::string> kIndonesianStopWords{
    "yang",     "dan",      "di",        "dari",      "ini",       "pada",
    "kepada",   "ada",      "adalah",    "dengan",    "untuk",     "dalam",
    "oleh",     "sebagai",  "juga",      "ke",        "atau",      "tidak",
    "itu",      "sebuah",   "tersebut",  "dapat",     "ia",        "telah",
    "satu",     "memiliki", "mereka",    "bahwa",     "lebih",     "karena",
    "seorang",  "akan",     "seperti",   "secara",    "kemudian",  "beberapa",
    "banyak",   "antara",   "setelah",   "yaitu",     "hanya",     "hingga",
    "serta",    "sama",     "dia",       "tetapi",    "namun",     "melalui",
    "bisa",     "sehingga", "ketika",    "suatu",     "sendiri",   "bagi",
    "semua",    "harus",    "setiap",    "maka",      "maupun",    "tanpa",
    "saja",     "jika",     "bukan",     "belum",     "sedangkan", "yakni",
    "meskipun", "hampir",   "kita",      "demikian",  "daripada",  "apa",
    "ialah",    "sana",     "begitu",    "seseorang", "selain",    "terlalu",
    "ataupun",  "saya",     "bila",      "bagaimana", "tapi",      "apabila",
    "kalau",    "kami",     "melainkan", "boleh",     "aku",       "anda",
    "kamu",     "beliau",   "kalian"};

// Arabic stop words (119 words)
inline const std::vector<std::string> kArabicStopWords{
    "من",   "ومن",  "منها",  "منه",  "في",   "وفي",  "فيها",  "فيه",  "و",
    "ف",    "ثم",   "او",    "أو",   "ب",    "بها",  "به",    "ا",    "أ",
    "اى",   "اي",   "أي",    "أى",   "لا",   "ولا",  "الا",   "ألا",  "إلا",
    "لكن",  "ما",   "وما",   "كما",  "فما",  "عن",   "مع",    "اذا",  "إذا",
    "ان",   "أن",   "إن",    "انها", "أنها", "إنها", "انه",   "أنه",  "إنه",
    "بان",  "بأن",  "فان",   "فأن",  "وان",  "وأن",  "وإن",   "التى", "التي",
    "الذى", "الذي", "الذين", "الى",  "الي",  "إلى",  "إلي",   "على",  "عليها",
    "عليه", "اما",  "أما",   "إما",  "ايضا", "أيضا", "كل",    "وكل",  "لم",
    "ولم",  "لن",   "ولن",   "هى",   "هي",   "هو",   "وهى",   "وهي",  "وهو",
    "فهى",  "فهي",  "فهو",   "انت",  "أنت",  "لك",   "لها",   "له",   "هذه",
    "هذا",  "تلك",  "ذلك",   "هناك", "كانت", "كان",  "يكون",  "تكون", "وكانت",
    "وكان", "غير",  "بعض",   "قد",   "نحو",  "بين",  "بينما", "منذ",  "ضمن",
    "حيث",  "الان", "الآن",  "خلال", "بعد",  "قبل",  "حتى",   "عند",  "عندما",
    "لدى",  "جميع"};

// Turkish stop words (209 words)
inline const std::vector<std::string> kTurkishStopWords{
    "acaba",      "altmış",      "altı",      "ama",         "ancak",
    "arada",      "aslında",     "ayrıca",    "bana",        "bazı",
    "belki",      "ben",         "benden",    "beni",        "benim",
    "beri",       "beş",         "bile",      "bin",         "bir",
    "birçok",     "biri",        "birkaç",    "birkez",      "birşey",
    "birşeyi",    "biz",         "bize",      "bizden",      "bizi",
    "bizim",      "böyle",       "böylece",   "bu",          "buna",
    "bunda",      "bundan",      "bunlar",    "bunları",     "bunların",
    "bunu",       "bunun",       "burada",    "çok",         "çünkü",
    "da",         "daha",        "dahi",      "de",          "defa",
    "değil",      "diğer",       "diye",      "doksan",      "dokuz",
    "dolayı",     "dolayısıyla", "dört",      "edecek",      "eden",
    "ederek",     "edilecek",    "ediliyor",  "edilmesi",    "ediyor",
    "eğer",       "elli",        "en",        "etmesi",      "etti",
    "ettiği",     "ettiğini",    "gibi",      "göre",        "halen",
    "hangi",      "hatta",       "hem",       "henüz",       "hep",
    "hepsi",      "her",         "herhangi",  "herkesin",    "hiç",
    "hiçbir",     "için",        "iki",       "ile",         "ilgili",
    "ise",        "işte",        "itibaren",  "itibariyle",  "kadar",
    "karşın",     "katrilyon",   "kendi",     "kendilerine", "kendini",
    "kendisi",    "kendisine",   "kendisini", "kez",         "ki",
    "kim",        "kimden",      "kime",      "kimi",        "kimse",
    "kırk",       "milyar",      "milyon",    "mu",          "mü",
    "mı",         "nasıl",       "ne",        "neden",       "nedenle",
    "nerde",      "nerede",      "nereye",    "niye",        "niçin",
    "o",          "olan",        "olarak",    "oldu",        "olduğu",
    "olduğunu",   "olduklarını", "olmadı",    "olmadığı",    "olmak",
    "olması",     "olmayan",     "olmaz",     "olsa",        "olsun",
    "olup",       "olur",        "olursa",    "oluyor",      "on",
    "ona",        "ondan",       "onlar",     "onlardan",    "onları",
    "onların",    "onu",         "onun",      "otuz",        "oysa",
    "öyle",       "pek",         "rağmen",    "sadece",      "sanki",
    "sekiz",      "seksen",      "sen",       "senden",      "seni",
    "senin",      "siz",         "sizden",    "sizi",        "sizin",
    "şey",        "şeyden",      "şeyi",      "şeyler",      "şöyle",
    "şu",         "şuna",        "şunda",     "şundan",      "şunları",
    "şunu",       "tarafından",  "trilyon",   "tüm",         "üç",
    "üzere",      "var",         "vardı",     "ve",          "veya",
    "ya",         "yani",        "yapacak",   "yapılan",     "yapılması",
    "yapıyor",    "yapmak",      "yaptı",     "yaptığı",     "yaptığını",
    "yaptıkları", "yedi",        "yerine",    "yetmiş",      "yine",
    "yirmi",      "yoksa",       "yüz",       "zaten"};

// Returns the default stop word list for the given language.
// Returns English stop words for LANGUAGE_UNSPECIFIED.
inline const std::vector<std::string>& GetDefaultStopWords(
    data_model::Language language) {
  switch (language) {
    case data_model::LANGUAGE_FRENCH:
      return kFrenchStopWords;
    case data_model::LANGUAGE_GERMAN:
      return kGermanStopWords;
    case data_model::LANGUAGE_SPANISH:
      return kSpanishStopWords;
    case data_model::LANGUAGE_ITALIAN:
      return kItalianStopWords;
    case data_model::LANGUAGE_PORTUGUESE:
      return kPortugueseStopWords;
    case data_model::LANGUAGE_RUSSIAN:
      return kRussianStopWords;
    case data_model::LANGUAGE_SWEDISH:
      return kSwedishStopWords;
    case data_model::LANGUAGE_TURKISH:
      return kTurkishStopWords;
    case data_model::LANGUAGE_DUTCH:
      return kDutchStopWords;
    case data_model::LANGUAGE_INDONESIAN:
      return kIndonesianStopWords;
    case data_model::LANGUAGE_ARABIC:
      return kArabicStopWords;
    case data_model::LANGUAGE_ENGLISH:
    case data_model::LANGUAGE_UNSPECIFIED:
    default:
      return kEnglishStopWords;
  }
}

// Build a stop words hash set from a vector. Lowercases all entries.
inline absl::flat_hash_set<std::string> BuildStopWordsSet(
    const std::vector<std::string>& stop_words) {
  absl::flat_hash_set<std::string> stop_words_set;
  for (const auto& word : stop_words) {
    stop_words_set.insert(absl::AsciiStrToLower(word));
  }
  return stop_words_set;
}

}  // namespace valkey_search::indexes::text

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_STOP_WORDS_H_
