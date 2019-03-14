package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
)

type Movies struct {
	Name      string
	Director  string
	Year      int
	Genres    string
	Rate      float64
	Thumbnail string
}

type Game struct {
	Name string
	Platform string
	Genres string
	Developer string
	Year string
	Cover string
}

func getMovieArray() []Movies {
	var movieArray = []Movies{{"Como entrenar a tu dragon 3", "Dean DeBlois", 2019, "Animación, Acción, Aventura", 7.9, "https://m.media-amazon.com/images/M/MV5BMjIwMDIwNjAyOF5BMl5BanBnXkFtZTgwNDE1MDc2NTM@._V1_UX140_CR0,0,140,209_AL_.jpg"},
		{"Glass", "M. Night Shyamalan", 2019, "Drama, Ciencia Ficción, Thriller", 7.0, "https://m.media-amazon.com/images/M/MV5BMTY1OTA2MjI5OV5BMl5BanBnXkFtZTgwNzkxMjU4NjM@._V1_UY209_CR3,0,140,209_AL_.jpg"},
		{"The Upside", "Neil Burger", 2017, "Comedia, Drama", 6.3, "https://m.media-amazon.com/images/M/MV5BNzY3NzYyNjI0N15BMl5BanBnXkFtZTgwNjYzMDc0NjM@._V1_UY209_CR0,0,140,209_AL_.jpg"},
		{"Aquaman", "James Wan", 2018, "Acción, Aventura, Fantasia", 7.4, "https://m.media-amazon.com/images/M/MV5BOTk5ODg0OTU5M15BMl5BanBnXkFtZTgwMDQ3MDY3NjM@._V1_UY209_CR0,0,140,209_AL_.jpg"},
		{"Spiderman: Un nuevo universo", "Peter Ramsey", 2018, "Animación, Acción, Aventura", 8.3, "https://m.media-amazon.com/images/M/MV5BMjMwNDkxMTgzOF5BMl5BanBnXkFtZTgwNTkwNTQ3NjM@._V1_UY209_CR0,0,140,209_AL_.jpg"},
		{"Escape Room", "Adam Robitel", 2019, "Acción, Aventura, Drama. Horror", 6.4, "https://m.media-amazon.com/images/M/MV5BMjQ2NDMwMTY3MF5BMl5BanBnXkFtZTgwNDg5OTc1NjM@._V1_UY209_CR0,0,140,209_AL_.jpg"},
		{"Uno más de la familia", "Charles Martin Smith", 2019, "Aventura, Familia", 6.5, "https://m.media-amazon.com/images/M/MV5BMTg5MjcwMzY5OV5BMl5BanBnXkFtZTgwMDM0OTI1NjM@._V1_UY209_CR0,0,140,209_AL_.jpg"},
		{"Green Book", "Peter Farrelly", 2018, "Biografía, Comedia, Drama", 8.3, "https://m.media-amazon.com/images/M/MV5BMjMyNzExNzQ5OV5BMl5BanBnXkFtZTgwNjM2MjIxNjM@._V1_UX140_CR0,0,140,209_AL_.jpg"},
		{"El niño que pudo ser rey", "Joe Cornish", 2019, "Acción, Aventura, Familia, Fantasía", 6.3, "https://m.media-amazon.com/images/M/MV5BMjMzOTUwNzgyOV5BMl5BanBnXkFtZTgwNjk3MTQwNzM@._V1_UY209_CR0,0,140,209_AL_.jpg"},
		{"Total Dhamaal", "Indra Kumar", 2019, "Acción, Aventura, Comedia", 0.0, "https://m.media-amazon.com/images/M/MV5BMTdiN2Q2MGUtYWRjMi00M2Y2LWEzOTYtOTA3NjNiMGMzNmFhXkEyXkFqcGdeQXVyNjE1OTQ0NjA@._V1_UY209_CR4,0,140,209_AL_.jpg"}}

	return movieArray
}

func movieHandler(w http.ResponseWriter, r *http.Request) {
	var database = openDatabase()
	rows, _ := database.Query("SELECT name, director, year, genres, rate, thumbnail FROM movies")
	var movies = sqlToStruct(rows)
	var moviesJson = moviesToJson(movies)
	fmt.Fprintf(w, "%s", moviesJson)
	database.Close()
}

func gameHandler(w http.ResponseWriter, r *http.Request) {
	database := openDatabase()
	rows, _ := database.Query("SELECT name, platform, genres, developer, year, cover FROM games")
	var games = sqlGameToStruct(rows)
	var gamesJson = gamesToJson(games)
	fmt.Fprintf(w, "%s", gamesJson)
	database.Close()
}

func sqlToStruct(rows *sql.Rows) []Movies {
	var movies = make([]Movies, 0)
	for rows.Next() {
		var movie = Movies{}
		rows.Scan(&movie.Name, &movie.Director, &movie.Year, &movie.Genres, &movie.Rate, &movie.Thumbnail)
		movies = append(movies, movie)
	}
	return movies
}

func sqlGameToStruct(rows *sql.Rows) []Game {
	var games = make([]Game, 0)
	for rows.Next() {
		var game = Game{}
		rows.Scan(&game.Name, &game.Platform, &game.Genres, &game.Developer, &game.Year, &game.Cover)
		games = append(games, game)
	}
	return games
}

func moviesToJson(movies []Movies) string {
	response, err := json.Marshal(movies)
	if err != nil {
		fmt.Println(err)
	}
	return string(response)
}

func gamesToJson(games []Game) string {
	response, err := json.Marshal(games)
	if err != nil {
		fmt.Println(err)
	}
	return string(response)
}

func insertMovies(movies []Movies) {
	var database = openDatabase()
	for i := 0; i < len(movies); i++ {
		var movie = movies[i]
		var statement, _ = database.Prepare("INSERT INTO movies (name, director, year, genres, rate, thumbnail) VALUES (?, ?, ?, ?, ?, ?)")
		statement.Exec(movie.Name, movie.Director, movie.Year, movie.Genres, movie.Rate, movie.Thumbnail)
	}
	database.Close()
}

func insertGames(games []Game){
	var database = openDatabase()
	for i := 0; i<len(games) ; i++ {
		var game = games[i]
		statement, _ := database.Prepare("INSERT INTO games (name, platform, genres, developer, year, cover) VALUES (?, ?, ?, ?, ?, ?)")
		statement.Exec(game.Name, game.Platform, game.Genres, game.Developer, game.Year, game.Cover)
	}
	database.Close()
}

func openDatabase() *sql.DB {
	database, _ := sql.Open("sqlite3", "examen.db")
	return database
}

func addMovieHandler(w http.ResponseWriter, r *http.Request) {
	var query= r.URL.Query()
	fmt.Println(query)
	var year int
	var rate float64
	var err error

	if len(query["year"])>0 {
		year, err = strconv.Atoi(query["year"][0])
		if err!=nil  {
			year = 2019
		}
	}else{
		fmt.Fprint(w, "Error en el año de la película.")
		return
	}
	if len(query["rate"])>0 {
		rate, err = strconv.ParseFloat(query["rate"][0], 64)
		if err!=nil {
			rate = 0.0
		}
	}else{
		fmt.Fprint(w, "Error en la puntuación de la película.")
		return
	}
	var name string
	if len(query["name"])>0 {
		name = query["name"][0]
	}else{
		fmt.Fprint(w, "Error en el nombre de la película.")
		return
	}
	var director string
	if len(query["director"])>0 {
		director = query["director"][0]
	}else{
		fmt.Fprint(w, "Error en el director de la película.")
		return
	}
	var genres string
	if len(query["genres"])>0 {
		genres = query["genres"][0]
	}else{
		fmt.Fprint(w, "Error en los géneros de la película.")
		return
	}
	var thumbnail string
	if len(query["thumbnail"])>0 {
		thumbnail = query["thumbnail"][0]
	}else{
		thumbnail = ""
	}

	var newMovie= Movies{name, director, year, genres, rate, thumbnail}
	var movies = []Movies{newMovie}
	insertMovies(movies)

	out, err := json.Marshal(newMovie)
	fmt.Fprintf(w, string(out))
}

// Method to read games.csv file and return a slice of Game struct
func readCsvGames() []Game {
	fileName := "games.csv"
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("OPENING CSV")
		fmt.Println(err)
		os.Exit(1)
	}

	r := csv.NewReader(file)
	lines, err := r.ReadAll()
	if err != nil {
		fmt.Println("READING CSV")
		fmt.Println(err)
		os.Exit(1)
	}
	games := parseLines(lines)
	fmt.Println(games)
	return  games
}

func parseLines(lines [][]string) []Game {
	ret := make([]Game, len(lines))
	for i, line := range lines {
		ret[i] = Game{
			Name: line[0],
			Platform: line[1],
			Genres: line[2],
			Developer: line[3],
			Year: line[4],
			Cover: line[5],
		}
	}
	return ret
}

func main() {
	var database = openDatabase()
	statement, _ := database.Prepare("CREATE TABLE IF NOT EXISTS movies (id INTEGER PRIMARY KEY, name TEXT, director TEXT, year INTEGER, genres TEXT, rate FLOAT, thumbnail TEXT)")
	_, e := statement.Exec()
	if e != nil {
		fmt.Println(e)
	}
	statement, _ = database.Prepare("CREATE TABLE IF NOT EXISTS games (id INTEGER PRIMARY KEY, name TEXT, platform TEXT, genres TEXT, developer TEXT, year TEXT, cover TEXT)")
	_, e = statement.Exec()
	if e != nil {
		fmt.Println(e)
	}

	rows, _ := database.Query("SELECT name, director, year, genres, rate, thumbnail FROM movies")
	var movies = sqlToStruct(rows)


	rowsG, _ := database.Query("SELECT name, platform, genres, developer, year, cover FROM games")
	var games = sqlGameToStruct(rowsG)
	if len(games) == 0 {
		var gameArray = readCsvGames()
		insertGames(gameArray)
	}

	database.Close()

	if len(movies) == 0 {
		var movieArray = getMovieArray()
		insertMovies(movieArray)
	}

	for i := 0; i < len(movies); i++ {
		fmt.Println(movies[i].Name + " " + strconv.Itoa(movies[i].Year))
	}
	for i := 0; i < len(games); i++ {
		fmt.Println(games[i])
	}

	http.HandleFunc("/movies", movieHandler)
	http.HandleFunc("/movies/add", addMovieHandler)

	http.HandleFunc("/games", gameHandler)

	log.Fatal(http.ListenAndServe(":8080", nil))
}
